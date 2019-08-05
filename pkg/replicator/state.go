package replicator

import "sync/atomic"

type state struct {
	v uint32
}

type stateValue uint32

const (
	stateIdle stateValue = iota
	statePaused
	statePausing
)

var states = map[stateValue]string{
	stateIdle:    "IDLE",
	statePausing: "PAUSING",
	statePaused:  "PAUSED",
}

func (r *Replicator) pause() string {
	r.logger.Debugf("pausing")
	if state := r.curState.Load(); state == statePausing ||
		state == statePaused {
		return r.curState.String()
	}
	if !r.curState.CompareAndSwap(stateIdle, statePausing) {
		return r.curState.String()
	}
	r.inTxMutex.Lock()
	r.curState.Store(statePaused)
	r.logger.Debugf("paused")

	return "OK"
}

// resume should mind the pause in progress state
func (r *Replicator) resume() string {
	r.logger.Debugf("resuming")
	if !r.curState.CompareAndSwap(statePaused, stateIdle) {
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
