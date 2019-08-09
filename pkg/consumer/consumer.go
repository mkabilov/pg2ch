package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx"
	"go.uber.org/zap"

	"github.com/mkabilov/pg2ch/pkg/decoder"
	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/utils/dbtypes"
)

const (
	statusTimeout   = time.Second * 10
	replWaitTimeout = time.Second * 10
)

// Handler represents interface for processing logical replication messages
type Handler interface {
	HandleMessage(dbtypes.LSN, message.Message) error
}

// Interface represents interface for the consumer
type Interface interface {
	SendStatus() error
	Run(Handler) error
	AdvanceLSN(dbtypes.LSN)
	CurrentLSN() dbtypes.LSN
	Wait()
}

type consumer struct {
	sync.RWMutex

	waitGr          *sync.WaitGroup
	ctx             context.Context
	conn            *pgx.ReplicationConn
	dbCfg           pgx.ConnConfig
	slotName        string
	publicationName string
	logger          *zap.SugaredLogger

	currentLSNMutex *sync.RWMutex
	currentLSN      dbtypes.LSN
	errCh           chan error
}

// New instantiates the consumer
func New(ctx context.Context, logger *zap.SugaredLogger, errCh chan error, dbCfg pgx.ConnConfig, slotName, publicationName string, startLSN dbtypes.LSN) *consumer {
	return &consumer{
		waitGr:          &sync.WaitGroup{},
		ctx:             ctx,
		dbCfg:           dbCfg,
		slotName:        slotName,
		publicationName: publicationName,
		currentLSNMutex: &sync.RWMutex{},
		currentLSN:      startLSN,
		errCh:           errCh,
		logger:          logger,
	}
}

func (c *consumer) CurrentLSN() dbtypes.LSN {
	return c.currentLSN
}

// AdvanceLSN advances lsn position
func (c *consumer) AdvanceLSN(lsn dbtypes.LSN) {
	c.currentLSNMutex.Lock()
	defer c.currentLSNMutex.Unlock()

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
	defer c.logger.Sync()

	c.logger.Infof("starting decoding from %s lsn", c.currentLSN)
	c.currentLSNMutex.RLock()
	err := c.conn.StartReplication(c.slotName, uint64(c.currentLSN), -1,
		`"proto_version" '1'`, fmt.Sprintf(`"publication_names" '%s'`, c.publicationName))
	c.currentLSNMutex.RUnlock()
	if err != nil {
		c.closeDbConnection()
		return fmt.Errorf("failed to start decoding logical replication messages: %v", err)
	}

	c.waitGr.Add(1)
	go func() {
		defer c.waitGr.Done()
		tick := time.NewTicker(statusTimeout)
		defer tick.Stop()

		for {
			select {
			case <-tick.C:
				if err = c.SendStatus(); err != nil {
					return
				}

			case <-c.ctx.Done():
				return
			}
		}
	}()

	return nil
}

func (c *consumer) closeDbConnection() {
	defer c.logger.Sync()
	if err := c.conn.Close(); err != nil {
		c.logger.Warnf("could not close replication connection: %v", err)
	}
}

func (c *consumer) processReplicationMessage(handler Handler) {
	defer c.waitGr.Done()
	defer c.logger.Sync()

	for {
		select {
		case <-c.ctx.Done():
			c.closeDbConnection()
			return
		default:
			wctx, cancel := context.WithTimeout(c.ctx, replWaitTimeout)
			c.Lock()
			repMsg, err := c.conn.WaitForReplicationMessage(wctx)
			c.Unlock()
			cancel()

			if err == context.DeadlineExceeded {
				continue
			} else if err == context.Canceled {
				c.logger.Infof("received shutdown request: decoding terminated")
				return
			} else if err != nil {
				// TODO: make sure we retry and cleanup after ourselves afterwards
				c.close(fmt.Errorf("replication failed: %v", err))
				return
			}

			if repMsg == nil {
				c.logger.Debugf("received null replication message")
				continue
			}

			if repMsg.WalMessage != nil {
				msg, err := decoder.Parse(repMsg.WalMessage.WalData)
				if err != nil {
					c.close(fmt.Errorf("invalid pgoutput message: %s", err))
					return
				}

				if err := handler.HandleMessage(dbtypes.LSN(repMsg.WalMessage.WalStart), msg); err != nil {
					c.close(fmt.Errorf("error handling waldata: %s", err))
					return
				}
			}

			if repMsg.ServerHeartbeat != nil && repMsg.ServerHeartbeat.ReplyRequested == 1 {
				c.logger.Debugf("server wants a reply")
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
	c.Lock()
	defer c.Unlock()
	defer c.logger.Sync()

	c.currentLSNMutex.RLock()
	c.logger.Debugf("sending status: %v", c.currentLSN)
	status, err := pgx.NewStandbyStatus(uint64(c.currentLSN))
	c.currentLSNMutex.RUnlock()

	if err != nil {
		return fmt.Errorf("error creating standby status: %s", err)
	}

	if err := c.conn.SendStandbyStatus(status); err != nil {
		return fmt.Errorf("failed to send standy status: %s", err)
	}

	return nil
}
