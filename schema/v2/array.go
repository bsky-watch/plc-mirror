package v2

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"bsky.watch/plc-mirror/util/plc"
)

type EntryLog []plc.OperationLogEntry

func (e *EntryLog) Scan(src any) error {
	b := []byte{}
	switch src := src.(type) {
	case []byte:
		b = src
	case string:
		b = []byte(src)
	default:
		return fmt.Errorf("unsupported type %T", src)
	}

	s := []string{}
	if b[0] == '{' {
		b[0] = '['
	}
	if b[len(b)-1] == '}' {
		b[len(b)-1] = ']'
	}
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("unmarshaling array: %w", err)
	}

	entries := EntryLog{}
	for i, v := range s {
		var entry plc.OperationLogEntry
		if err := json.Unmarshal([]byte(v), &entry); err != nil {
			return fmt.Errorf("unmarshaling array entry %d: %w", i, err)
		}
		entries = append(entries, entry)
	}
	*e = entries
	return nil
}

func (e EntryLog) GormValue(ctx context.Context, db *gorm.DB) clause.Expr {
	r := clause.Expr{}
	r.SQL = fmt.Sprintf("array[%s]::jsonb[]", strings.Join(slices.Repeat([]string{"?::jsonb"}, len(e)), ", "))
	r.Vars = mapSlice(e, func(v plc.OperationLogEntry) interface{} { return v })
	return r
}

func mapSlice[A any, B any](s []A, fn func(A) B) []B {
	r := make([]B, 0, len(s))
	for _, v := range s {
		r = append(r, fn(v))
	}
	return r
}
