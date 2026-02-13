package pglock

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Lock struct {
	pool      *pgxpool.Pool
	conn      *pgxpool.Conn
	lockID    int64
	lockCount int
	err       error
}

func New(pool *pgxpool.Pool, id int64) (*Lock, error) {
	return &Lock{
		pool:   pool,
		lockID: id,
	}, nil
}

func (l *Lock) Lock(ctx context.Context) error {
	if l.conn == nil {
		conn, err := l.pool.Acquire(ctx)
		if err != nil {
			return err
		}
		l.conn = conn
		l.lockCount = 0
		l.err = nil
	}

	_, err := l.conn.Exec(ctx, "select pg_advisory_lock($1)", l.lockID)
	if err != nil {
		return fmt.Errorf("acquiring lock: %w", err)
	}
	l.lockCount++

	return nil
}

func (l *Lock) TryLock(ctx context.Context) (bool, error) {
	if l.conn == nil {
		conn, err := l.pool.Acquire(ctx)
		if err != nil {
			return false, err
		}
		l.conn = conn
		l.lockCount = 0
		l.err = nil
	}

	var result bool
	err := l.conn.QueryRow(ctx, "select pg_try_advisory_lock($1)", l.lockID).Scan(&result)
	if err != nil {
		return false, fmt.Errorf("acquiring lock: %w", err)
	}
	if result {
		l.lockCount++
		return true, nil
	}

	return false, nil
}

func (l *Lock) LockWithTimeout(ctx context.Context, timeout time.Duration) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	err := l.Lock(ctx)
	if err != nil {
		if err == context.DeadlineExceeded {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func (l *Lock) Unlock(ctx context.Context) error {
	if l.conn == nil || l.lockCount == 0 {
		return fmt.Errorf("not locked")
	}

	_, err := l.conn.Exec(ctx, "select pg_advisory_unlock($1)", l.lockID)
	if err != nil {
		return fmt.Errorf("releasing lock: %w", err)
	}
	l.lockCount--

	if l.lockCount == 0 {
		l.conn.Release()
		l.conn = nil
	}

	return nil
}

func (l *Lock) Check(ctx context.Context) (bool, error) {
	if l.err != nil {
		return false, l.err
	}

	if l.conn == nil || l.lockCount == 0 {
		return false, nil
	}

	// Postgres releases advisory locks when the connection ends, so if
	// lockCount > 0 and the connection is still alive - we are still holding
	// the lock.
	err := l.conn.Ping(ctx)
	if err != nil {
		l.err = err
		return false, err
	}

	return true, nil
}

func (l *Lock) Reset(ctx context.Context) {
	if l.conn != nil {
		// Closing ensures that any still held locks are released.
		_ = l.conn.Conn().Close(ctx)

		l.conn.Release()
	}
	l.conn = nil
	l.lockCount = 0
	l.err = nil
}
