package dedup

import (
	"context"
	"time"

	"github.com/K0ngS3ng/CertStreamerPro/internal/store"
)

func RunPendingEmitter(ctx context.Context, st store.Store, out chan<- Event, logf func(string, ...any)) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = st.IteratePending(1000, func(key string, ev store.PendingEvent) bool {
				select {
				case <-ctx.Done():
					return false
				case out <- Event{Domain: ev.Domain, LogURL: ev.LogURL, Observed: ev.Observed, SCTime: ev.SCTime}:
					_ = st.DeletePending(key)
					return true
				default:
					if logf != nil {
						logf("output channel full, pausing pending emit")
					}
					return false
				}
			})
		}
	}
}
