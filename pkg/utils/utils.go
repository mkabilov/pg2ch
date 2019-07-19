package utils

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"
)

//ErrAttemptsExceeded represents attempts exceeded error
var ErrAttemptsExceeded = errors.New("attempts exceeded")

//Try tries to run {fn} {maxAttempts} times with {attemptInterval} between attempts
func Try(ctx context.Context, maxAttempts int, attemptInterval time.Duration, fn func() error) error {
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := fn(); err == nil {
			return nil
		} else {
			log.Printf("try: %v", err)
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("abort retrying")
		case <-time.After(attemptInterval):
		}
	}

	return ErrAttemptsExceeded
}
