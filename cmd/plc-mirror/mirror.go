package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/time/rate"
	"gorm.io/gorm"

	"bsky.watch/plc-mirror/schema"
	"bsky.watch/plc-mirror/util/pglock"
	"bsky.watch/plc-mirror/util/plc"
)

const (
	// Current rate limit is `500 per five minutes`, lets stay a bit under it.
	defaultRateLimit  = rate.Limit(450.0 / 300)
	caughtUpRateLimit = rate.Limit(0.2)
	caughtUpThreshold = 10 * time.Minute
)

type Mirror struct {
	db       schema.Database
	dbUrl    string
	upstream *url.URL
	limiter  *rate.Limiter
	lockID   int64

	mu                      sync.RWMutex
	lastCompletionTimestamp time.Time
}

func NewMirror(ctx context.Context, cfg Config, db schema.Database) (*Mirror, error) {
	u, err := url.Parse(cfg.Upstream)
	if err != nil {
		return nil, err
	}
	u.Path, err = url.JoinPath(u.Path, "export")
	if err != nil {
		return nil, err
	}
	r := &Mirror{
		db:       db,
		upstream: u,
		limiter:  rate.NewLimiter(defaultRateLimit, 4),
		lockID:   cfg.LockID,
		dbUrl:    cfg.DBUrl,
	}
	return r, nil
}

func (m *Mirror) Start(ctx context.Context, leaderLock *pglock.Lock) error {
	go m.run(ctx, leaderLock)
	return nil
}

func (m *Mirror) run(ctx context.Context, leaderLock *pglock.Lock) {
	log := zerolog.Ctx(ctx).With().Str("module", "mirror").Logger()
	for {
		select {
		case <-ctx.Done():
			log.Info().Msgf("PLC mirror stopped")
			return
		default:
			isLeader, err := leaderLock.Check(ctx)
			if err != nil {
				log.Error().Err(err).Msgf("Failed to check leader election status: %s", err)

				leaderLock.Reset(ctx)

				time.Sleep(10 * time.Second)
				break
			}

			if !isLeader {
				r, err := leaderLock.TryLock(ctx)
				if err != nil {
					log.Error().Err(err).Msgf("Failed to acquire leader lock: %s", err)
					break
				}
				isLeader = r
				if isLeader {
					log.Info().Msgf("Became the leader")
				} else {
					time.Sleep(10 * time.Second)
				}
			}

			if isLeader {
				if err := m.runOnce(ctx, leaderLock); err != nil {
					if ctx.Err() == nil {
						log.Error().Err(err).Msgf("Failed to get new log entries from PLC: %s", err)
					}
				} else {
					now := time.Now()
					m.mu.Lock()
					m.lastCompletionTimestamp = now
					m.mu.Unlock()
				}
				time.Sleep(10 * time.Second)
			}
		}
	}
}

func (m *Mirror) LastCompletion() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastCompletionTimestamp
}

func (m *Mirror) LastRecordTimestamp(ctx context.Context) (time.Time, error) {
	ts, err := m.db.HeadTimestamp(ctx)
	if err != nil {
		return time.Time{}, err
	}
	dbTimestamp, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return time.Time{}, fmt.Errorf("parsing timestamp %q: %w", ts, err)
	}
	return dbTimestamp, nil
}

func (m *Mirror) updateRateLimit(lastRecordTimestamp time.Time) {
	// Reduce rate limit if we are caught up, to get new records in larger batches.
	desiredRate := defaultRateLimit
	if time.Since(lastRecordTimestamp) < caughtUpThreshold {
		desiredRate = caughtUpRateLimit
	}
	if math.Abs(float64(m.limiter.Limit()-desiredRate)) > 0.0000001 {
		m.limiter.SetLimit(rate.Limit(desiredRate))
	}
}

func (m *Mirror) runOnce(ctx context.Context, leaderLock *pglock.Lock) error {
	log := zerolog.Ctx(ctx)

	cursor, err := m.db.HeadTimestamp(ctx)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return fmt.Errorf("failed to get the cursor: %w", err)
	}

	if err != nil {
		cursor = ""
	} else {
		cursorTimestamp, err := time.Parse(time.RFC3339, cursor)
		if err != nil {
			log.Error().Err(err).Msgf("parsing timestamp %q: %s", cursor, err)
		} else {
			m.updateRateLimit(cursorTimestamp)
		}
	}

	u := *m.upstream

	for {
		params := u.Query()
		params.Set("count", "1000")
		if cursor != "" {
			params.Set("after", cursor)
		}
		u.RawQuery = params.Encode()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return fmt.Errorf("constructing request: %w", err)
		}

		_ = m.limiter.Wait(ctx)
		log.Info().Msgf("Listing PLC log entries with cursor %q...", cursor)
		log.Debug().Msgf("Request URL: %s", u.String())
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return fmt.Errorf("sending request: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}

		newEntries := []plc.OperationLogEntry{}
		decoder := json.NewDecoder(resp.Body)
		oldCursor := cursor

		var lastTimestamp time.Time

		for {
			var entry plc.OperationLogEntry
			err := decoder.Decode(&entry)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return fmt.Errorf("parsing log entry: %w", err)
			}

			newEntries = append(newEntries, entry)

			t, err := time.Parse(time.RFC3339, entry.CreatedAt)
			if err == nil {
				lastEventTimestamp.Set(float64(t.Unix()))
				lastTimestamp = t
			} else {
				log.Warn().Msgf("Failed to parse %q: %s", entry.CreatedAt, err)
			}
		}

		if len(newEntries) == 0 {
			break
		}

		cursor = plc.NextCursor(newEntries)
		if cursor == oldCursor {
			// Shouldn't happen
			break
		}

		isLeader, err := leaderLock.Check(ctx)
		if err != nil {
			return fmt.Errorf("failed to check leadership status: %w", err)
		}
		if !isLeader {
			log.Warn().Msgf("Lost leadership status")
			return nil
		}

		err = m.db.AppendEntries(ctx, newEntries)
		if err != nil {
			return fmt.Errorf("inserting log entry into database: %w", err)
		}

		if !lastTimestamp.IsZero() {
			m.updateRateLimit(lastTimestamp)
		}

		log.Info().Msgf("Got %d log entries. New cursor: %q", len(newEntries), cursor)
	}
	return nil
}
