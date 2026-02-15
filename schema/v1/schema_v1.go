package v1

import (
	"context"
	"time"

	"bsky.watch/plc-mirror/models"
	"bsky.watch/plc-mirror/util/plc"
	"github.com/imax9000/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func IsActive(ctx context.Context, db *gorm.DB) (bool, error) {
	var entry PLCLogEntry
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

type PLCLogEntry struct {
	ID        models.ID `gorm:"primarykey"`
	CreatedAt time.Time

	DID          string        `gorm:"column:did;index:did_timestamp;uniqueIndex:did_cid"`
	CID          string        `gorm:"column:cid;uniqueIndex:did_cid"`
	PLCTimestamp string        `gorm:"column:plc_timestamp;index:did_timestamp,sort:desc;index:,sort:desc"`
	Nullified    bool          `gorm:"default:false"`
	Operation    plc.Operation `gorm:"type:JSONB;serializer:json"`
}

type Database struct {
	db *gorm.DB
}

func New(db *gorm.DB) *Database {
	return &Database{db: db}
}

func (d *Database) AutoMigrate() error {
	return d.db.AutoMigrate(&PLCLogEntry{})
}

func (d *Database) HeadTimestamp(ctx context.Context) (string, error) {
	ts := ""
	err := d.db.WithContext(ctx).Model(&PLCLogEntry{}).Select("plc_timestamp").Order("plc_timestamp desc").Limit(1).Take(&ts).Error
	return ts, err
}

func (d *Database) AppendEntries(ctx context.Context, entries []plc.OperationLogEntry) error {
	return d.db.Clauses(
		clause.OnConflict{
			Columns:   []clause.Column{{Name: "did"}, {Name: "cid"}},
			DoNothing: true,
		},
	).Create(mapSlice(entries, fromOperationLogEntry)).Error
}

func (d *Database) LastOperationForDID(ctx context.Context, did string) (*plc.OperationLogEntry, error) {
	var entry PLCLogEntry
	err := d.db.Model(&entry).Where("did = ? AND (NOT nullified)", did).Order("plc_timestamp desc").Limit(1).Take(&entry).Error
	if err != nil {
		return nil, err
	}

	r := &plc.OperationLogEntry{
		DID:       entry.DID,
		CID:       entry.CID,
		CreatedAt: entry.PLCTimestamp,
		Operation: entry.Operation,
		Nullified: entry.Nullified,
	}
	return r, nil
}

func fromOperationLogEntry(op plc.OperationLogEntry) PLCLogEntry {
	return PLCLogEntry{
		DID:          op.DID,
		CID:          op.CID,
		PLCTimestamp: op.CreatedAt,
		Nullified:    op.Nullified,
		Operation:    op.Operation,
	}
}

func mapSlice[A any, B any](s []A, fn func(A) B) []B {
	r := make([]B, 0, len(s))
	for _, v := range s {
		r = append(r, fn(v))
	}
	return r
}
