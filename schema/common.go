package schema

import (
	"context"
	"fmt"

	v1 "bsky.watch/plc-mirror/schema/v1"
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
	// Only one version exists now.
	r := v1.New(db)
	if err := r.AutoMigrate(); err != nil {
		return nil, fmt.Errorf("auto-migrating DB schema: %w", err)
	}
	return r, nil
}
