package consumer

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx"

	"github.com/mkabilov/pg2ch/pkg/decoder"
	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/utils"
)

const (
	statusTimeout   = time.Second * 10
	replWaitTimeout = time.Second * 10
)

// Handler represents interface for processing logical replication messages
type Handler interface {
	HandleMessage(message.Message, utils.LSN) error
}

// Interface represents interface for the consumer
type Interface interface {
	SendStatus() error
	Run(Handler) error
	AdvanceLSN(utils.LSN)
	Wait()
}

type consumer struct {
	waitGr          *sync.WaitGroup
	ctx             context.Context
	conn            *pgx.ReplicationConn
	dbCfg           pgx.ConnConfig
	slotName        string
	publicationName string
	currentLSN      utils.LSN
	errCh           chan error
}

// New instantiates the consumer
func New(ctx context.Context, errCh chan error, dbCfg pgx.ConnConfig, slotName, publicationName string, startLSN utils.LSN) *consumer {
	return &consumer{
		waitGr:          &sync.WaitGroup{},
		ctx:             ctx,
		dbCfg:           dbCfg,
		slotName:        slotName,
		publicationName: publicationName,
		currentLSN:      startLSN,
		errCh:           errCh,
	}
}

// AdvanceLSN advances lsn position
func (c *consumer) AdvanceLSN(lsn utils.LSN) {
	c.currentLSN = lsn
}

// Wait waits for the goroutines
func (c *consumer) Wait() {
	c.waitGr.Wait()
}

func (c *consumer) close(err error) {
	select {
	case c.errCh <- err:
	default:
	}
}

// Run runs consumer
func (c *consumer) Run(handler Handler) error {
	rc, err := pgx.ReplicationConnect(c.dbCfg)
	if err != nil {
		return fmt.Errorf("could not connect using replication protocol: %v", err)
	}

	c.conn = rc

	if err := c.startDecoding(); err != nil {
		return fmt.Errorf("could not start replication slot: %v", err)
	}

	// we may have flushed the final segment at shutdown without bothering to advance the slot LSN.
	if err := c.SendStatus(); err != nil {
		return fmt.Errorf("could not send replay progress: %v", err)
	}

	c.waitGr.Add(1)
	go c.processReplicationMessage(handler)

	return nil
}

func (c *consumer) startDecoding() error {
	log.Printf("Starting from %s lsn", c.currentLSN)

	err := c.conn.StartReplication(c.slotName, uint64(c.currentLSN), -1,
		`"proto_version" '1'`, fmt.Sprintf(`"publication_names" '%s'`, c.publicationName))

	if err != nil {
		c.closeDbConnection()
		return fmt.Errorf("failed to start decoding logical replication messages: %v", err)
	}

	return nil
}

func (c *consumer) closeDbConnection() {
	if err := c.conn.Close(); err != nil {
		log.Printf("could not close replication connection: %v", err)
	}
}

func (c *consumer) processReplicationMessage(handler Handler) {
	defer c.waitGr.Done()

	statusTicker := time.NewTicker(statusTimeout)
	for {
		select {
		case <-c.ctx.Done():
			statusTicker.Stop()
			c.closeDbConnection()
			return
		case <-statusTicker.C:
			if err := c.SendStatus(); err != nil {
				c.close(fmt.Errorf("could not send replay progress: %v", err))
				return
			}
		default:
			wctx, cancel := context.WithTimeout(c.ctx, replWaitTimeout)
			repMsg, err := c.conn.WaitForReplicationMessage(wctx)
			cancel()

			if err == context.DeadlineExceeded {
				continue
			} else if err == context.Canceled {
				log.Printf("received shutdown request: decoding terminated")
				return
			} else if err != nil {
				// TODO: make sure we retry and cleanup after ourselves afterwards
				c.close(fmt.Errorf("replication failed: %v", err))
				return
			}

			if repMsg == nil {
				log.Printf("received null replication message")
				continue
			}

			if repMsg.WalMessage != nil {
				msg, err := decoder.Parse(repMsg.WalMessage.WalData)
				if err != nil {
					c.close(fmt.Errorf("invalid pgoutput message: %s", err))
					return
				}

				if err := handler.HandleMessage(msg, utils.LSN(repMsg.WalMessage.WalStart)); err != nil {
					c.close(fmt.Errorf("error handling waldata: %s", err))
					return
				}
			}

			if repMsg.ServerHeartbeat != nil && repMsg.ServerHeartbeat.ReplyRequested == 1 {
				log.Println("server wants a reply")
				if err := c.SendStatus(); err != nil {
					c.close(fmt.Errorf("could not send replay progress: %v", err))
					return
				}
			}
		}
	}
}

// SendStatus sends the status
func (c *consumer) SendStatus() error {
	// log.Printf("sending status: %v", c.currentLSN) //TODO: move to debug log level
	status, err := pgx.NewStandbyStatus(uint64(c.currentLSN))

	if err != nil {
		return fmt.Errorf("error creating standby status: %s", err)
	}

	if err := c.conn.SendStandbyStatus(status); err != nil {
		return fmt.Errorf("failed to send standy status: %s", err)
	}

	return nil
}
