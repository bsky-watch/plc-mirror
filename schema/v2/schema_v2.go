package v2

import (
	"cmp"
	"context"
	"flag"
	"fmt"
	"slices"

	"bsky.watch/plc-mirror/util/plc"
	"github.com/imax9000/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	useTrigger = flag.Bool("schemav2-update-head-timestamp-with-trigger", true, "If set to true, head timestamp will be kept up to date using a PostgreSQL trigger, rather than from business logic. WARNING: if you run multiple replicas, while switching this off a few log entries might get duplicated.")
)

func IsActive(ctx context.Context, db *gorm.DB) (bool, error) {
	var entry DIDTableEntry
	err := db.WithContext(ctx).Limit(1).Take(&entry).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		if err, ok := errors.As[*pgconn.PgError](err); ok {
			if err.Code == "42P01" {
				return false, nil
			}
		}
		return false, err
	}
	return true, nil
}

type DIDTableEntry struct {
	DID string `gorm:"column:did;primarykey"`

	Log EntryLog `gorm:"type:JSONB[]"`
}

type HeadTimestamp struct {
	Timestamp string
}

func (DIDTableEntry) TableName() string {
	return "data"
}

func (HeadTimestamp) TableName() string {
	return "head_timestamp"
}

type Database struct {
	db *gorm.DB
}

func New(db *gorm.DB) *Database {
	return &Database{db: db}
}

func (d *Database) AutoMigrate() error {
	err := d.db.AutoMigrate(&DIDTableEntry{}, &HeadTimestamp{})
	if err != nil {
		return fmt.Errorf("auto-migration: %w", err)
	}

	if *useTrigger {
		if err := d.db.Exec(triggerFunction).Error; err != nil {
			return fmt.Errorf("creating trigger function: %w", err)
		}
		if err := d.db.Exec(installTrigger).Error; err != nil {
			return fmt.Errorf("installing the trigger: %w", err)
		}
	} else {
		if err := d.db.Exec(deleteTrigger).Error; err != nil {
			return fmt.Errorf("ensuring that the trigger is not installed: %w", err)
		}
	}

	// Ensure there's exactly one row in head_timestamp table.
	var count int64
	err = d.db.Model(&HeadTimestamp{}).Count(&count).Error
	if err != nil {
		return err
	}
	if count == 0 {
		err := d.db.Create(&HeadTimestamp{Timestamp: ""}).Error
		if err != nil {
			return err
		}
	}
	if count > 1 {
		err := d.db.Transaction(func(tx *gorm.DB) error {
			var maxTimestamp string
			err := tx.Model(&HeadTimestamp{}).Select("max(timestamp)").Take(&maxTimestamp).Error
			if err != nil {
				return err
			}
			return tx.Where("timestamp < ?", maxTimestamp).Delete(&HeadTimestamp{}).Error
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *Database) HeadTimestamp(ctx context.Context) (string, error) {
	var maxTimestamp string
	err := d.db.Model(&HeadTimestamp{}).Select("max(timestamp)").Take(&maxTimestamp).Error
	if err != nil {
		return "", err
	}
	if maxTimestamp == "" {
		return "", gorm.ErrRecordNotFound
	}
	return maxTimestamp, nil
}

func (d *Database) AppendEntries(ctx context.Context, entries []plc.OperationLogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	headTimestamp := plc.NextCursor(entries)
	if headTimestamp == "" {
		return fmt.Errorf("failed to get the new head timestamp")
	}

	// Sort in the reverse order, so that while iterating over the list
	// and append()'ing to per-DID slices the newest entry will be
	// at the front.
	slices.SortFunc(entries, func(a plc.OperationLogEntry, b plc.OperationLogEntry) int {
		return -cmp.Compare(a.CreatedAt, b.CreatedAt)
	})

	entryMap := map[string][]plc.OperationLogEntry{}
	for _, entry := range entries {
		did := entry.DID
		entry.DID = ""
		entryMap[did] = append(entryMap[did], entry)
	}

	rows := make([]DIDTableEntry, 0, len(entryMap))
	for did, entries := range entryMap {
		rows = append(rows, DIDTableEntry{DID: did, Log: entries})
	}

	return d.db.Transaction(func(tx *gorm.DB) error {
		if !*useTrigger {
			err := tx.Raw("update head_timestamp set timestamp = ? where timestamp < ?", headTimestamp, headTimestamp).Error
			if err != nil {
				return fmt.Errorf("updating head timestamp: %w", err)
			}
		}

		return tx.Clauses(
			clause.OnConflict{
				Columns: []clause.Column{{Name: "did"}},
				DoUpdates: clause.Assignments(map[string]interface{}{
					"log": gorm.Expr("array_cat(EXCLUDED.log, data.log)"),
				}),
			},
		).Create(rows).Error
	})
}

func (d *Database) LastOperationForDID(ctx context.Context, did string) (*plc.OperationLogEntry, error) {
	var entry DIDTableEntry
	if err := d.db.First(&entry, "did = ?", did).Error; err != nil {
		return nil, err
	}
	if len(entry.Log) == 0 {
		return nil, fmt.Errorf("no log entries present in the database")
	}
	r := entry.Log[0]
	r.DID = entry.DID
	return &r, nil
}
