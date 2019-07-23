package replicator

import "sync/atomic"

var states = map[uint32]string{
	stateIdle:            "IDLE",
	stateInactivityFlush: "INACTIVITY FLUSH",
	statePausing:         "PAUSING",
	statePaused:          "PAUSED",
}

func (r *Replicator) pause() string {
	r.logger.Debugf("pausing")
	if state := atomic.LoadUint32(&r.state); state == statePausing ||
		state == statePaused ||
		state == stateInactivityFlush {
		return r.status()
	}
	atomic.StoreUint32(&r.state, statePausing)
	r.inTxMutex.Lock()
	atomic.StoreUint32(&r.state, statePaused)
	r.logger.Debugf("paused")

	return "OK"
}

// resume should mind the pause in progress state
func (r *Replicator) resume() string {
	r.logger.Debugf("resuming")
	if !atomic.CompareAndSwapUint32(&r.state, statePaused, stateIdle) {
		if atomic.LoadUint32(&r.state) == statePausing {
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

func (r *Replicator) status() string {
	stateName, ok := states[atomic.LoadUint32(&r.state)]
	if !ok {
		return "UNKNOWN"
	}

	return stateName
}
