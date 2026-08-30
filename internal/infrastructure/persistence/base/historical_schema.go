package base

import "strings"

// HistoricalSchema preserves the exact released v1/v2 DDL bytes. Concrete
// database engines initialize this profile once; schema assembly never
// branches on a driver name.
type HistoricalSchema struct {
	TextType, KeyType, LargeType string
	QualifySchema                bool
}

func (profile HistoricalSchema) Table(schema, table string) string {
	if profile.QualifySchema && strings.TrimSpace(schema) != "" {
		return strings.TrimSpace(schema) + "." + table
	}
	return table
}
