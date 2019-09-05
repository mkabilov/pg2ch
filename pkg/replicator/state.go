package replicator

import "sync/atomic"

type state struct {
	v uint32
}

type StateValue uint32

const (
	StateInit StateValue = iota
	StateWorking
	StatePaused
	StatePausing
	StateShuttingDown
)

var states = map[StateValue]string{
	StateInit:    "INIT",
	StateWorking: "WORKING",
	StatePausing: "PAUSING",
	StatePaused:  "PAUSED",
}

func (r *Replicator) State() StateValue {
	return StateValue(r.curState.v)
}

func (r *Replicator) pause() string {
	r.logger.Debugf("pausing")
	if state := r.curState.Load(); state == StatePausing ||
		state == StatePaused {
		return r.curState.String()
	}
	if !r.curState.CompareAndSwap(StateWorking, StatePausing) {
		return r.curState.String()
	}
	r.inTxMutex.Lock()

	/* try to flush everying we have buffered */
	if err := r.tblBuffersFlush(); err != nil {
		r.curState.Store(StateWorking)
		r.inTxMutex.Unlock()
		return "could not flush tables"
	}

	r.curState.Store(StatePaused)
	r.logger.Debugf("paused")

	return "OK"
}

// resume should mind the pause in progress state
func (r *Replicator) resume() string {
	r.logger.Debugf("resuming")
	if !r.curState.CompareAndSwap(StatePaused, StateWorking) {
		if r.curState.Load() == StatePausing {
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

func (s *state) Load() StateValue {
	return StateValue(atomic.LoadUint32(&s.v))
}

func (s *state) CompareAndSwap(old, new StateValue) bool {
	return atomic.CompareAndSwapUint32(&s.v, uint32(old), uint32(new))
}

func (s *state) Store(new StateValue) {
	atomic.StoreUint32(&s.v, uint32(new))
}
