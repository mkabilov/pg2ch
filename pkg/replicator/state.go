package replicator

import "sync/atomic"

type state struct {
	v uint32
}

type stateValue uint32

const (
	stateInit stateValue = iota
	stateWorking
	statePaused
	statePausing
	stateShuttingDown
)

var states = map[stateValue]string{
	stateInit:    "INIT",
	stateWorking: "WORKING",
	statePausing: "PAUSING",
	statePaused:  "PAUSED",
}

func (r *Replicator) State() string {
	return r.curState.String()
}

func (r *Replicator) pause() string {
	r.logger.Debugf("pausing")
	if state := r.curState.Load(); state == statePausing ||
		state == statePaused {
		return r.curState.String()
	}
	if !r.curState.CompareAndSwap(stateWorking, statePausing) {
		return r.curState.String()
	}
	r.inTxMutex.Lock()

	/* try to flush everying we have buffered */
	if err := r.tblBuffersFlush(); err != nil {
		r.curState.Store(stateWorking)
		r.inTxMutex.Unlock()
		return "could not flush tables"
	}

	r.curState.Store(statePaused)
	r.logger.Debugf("paused")

	return "OK"
}

// resume should mind the pause in progress state
func (r *Replicator) resume() string {
	r.logger.Debugf("resuming")
	if !r.curState.CompareAndSwap(statePaused, stateWorking) {
		if r.curState.Load() == statePausing {
			r.logger.Debugf("pause is in progress")
			return "PAUSE IS IN PROGRESS"
		} else {
			r.logger.Debugf("not paused")
			return "NOT PAUSED"
		}
	}
	r.inTxMutex.Unlock()
	r.logger.Debugf("resumed")

	return "OK"
}

func (s *state) String() string {
	stateName, ok := states[s.Load()]
	if !ok {
		return "UNKNOWN"
	}

	return stateName
}

func (s *state) Load() stateValue {
	return stateValue(atomic.LoadUint32(&s.v))
}

func (s *state) CompareAndSwap(old, new stateValue) bool {
	return atomic.CompareAndSwapUint32(&s.v, uint32(old), uint32(new))
}

func (s *state) Store(new stateValue) {
	atomic.StoreUint32(&s.v, uint32(new))
}
