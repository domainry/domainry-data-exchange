// Package model owns durable worker state that is independent of SQL storage.
package model

import (
	"time"

	dataexchange "github.com/domainry/domainry-data-exchange-sdk"
)

type WorkItem struct {
	Job       dataexchange.Job
	Scope     dataexchange.Scope
	ObjectKey string
	Payload   []byte
	Attempts  int
}

type ArtifactRecord struct {
	ID, Filename, ContentType, SHA256 string
	Size                              int64
	ExpiresAt                         time.Time
}

type FailurePlan struct {
	Status        string
	Code          string
	Attempts      int
	NextAttemptAt time.Time
}
