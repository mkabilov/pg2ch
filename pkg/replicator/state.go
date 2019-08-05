package replicator

import "sync/atomic"

type state struct {
	v uint32
}

var states = map[uint32]string{
	stateIdle:            "IDLE",
	stateInactivityFlush: "INACTIVITY FLUSH",
	statePausing:         "PAUSING",
	statePaused:          "PAUSED",
}

func (r *Replicator) pause() string {
	r.logger.Debugf("pausing")
	if state := atomic.LoadUint32(&r.curState.v); state == statePausing ||
		state == statePaused ||
		state == stateInactivityFlush {
		return r.curState.String()
	}
	if !atomic.CompareAndSwapUint32(&r.curState.v, stateIdle, statePausing) {
		return r.curState.String()
	}
	r.inTxMutex.Lock()
	atomic.StoreUint32(&r.curState.v, statePaused)
	r.logger.Debugf("paused")

	return "OK"
}

// resume should mind the pause in progress state
func (r *Replicator) resume() string {
	r.logger.Debugf("resuming")
	if !atomic.CompareAndSwapUint32(&r.curState.v, statePaused, stateIdle) {
		if atomic.LoadUint32(&r.curState.v) == statePausing {
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
	stateName, ok := states[atomic.LoadUint32(&s.v)]
	if !ok {
		return "UNKNOWN"
	}

	return stateName
}
