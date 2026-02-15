package schema

import (
	"context"
	"fmt"

	v1 "bsky.watch/plc-mirror/schema/v1"
	v2 "bsky.watch/plc-mirror/schema/v2"
	"bsky.watch/plc-mirror/util/plc"
	"gorm.io/gorm"
)

type Database interface {
	HeadTimestamp(ctx context.Context) (string, error)
	AppendEntries(ctx context.Context, entries []plc.OperationLogEntry) error
	LastOperationForDID(ctx context.Context, did string) (*plc.OperationLogEntry, error)
	AutoMigrate() error
}

func DetectVersion(ctx context.Context, db *gorm.DB) (Database, error) {
	r, err := detectInternal(ctx, db)
	if err != nil {
		return nil, err
	}
	if err := r.AutoMigrate(); err != nil {
		return nil, fmt.Errorf("auto-migrating DB schema: %w", err)
	}
	return r, nil
}

func detectInternal(ctx context.Context, db *gorm.DB) (Database, error) {
	ok, err := v2.IsActive(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("checking iv v2 schema is in use: %w", err)
	}
	if ok {
		return v2.New(db), nil
	}

	ok, err = v1.IsActive(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("checking iv v1 schema is in use: %w", err)
	}
	if ok {
		return v1.New(db), nil
	}

	// If we reach this point, none of the known schemas are in use
	// and the DB is most likely empty. So just use the latest schema.
	return v2.New(db), nil
}
