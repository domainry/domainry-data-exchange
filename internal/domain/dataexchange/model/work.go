// Package model owns durable worker state that is independent of SQL storage.
package model

import (
	"strings"
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

// JobAccess is the storage-facing projection of one exact Permission grant.
// It is deliberately not a role-authoring or public RecordScope protocol.
// Job-management Permissions require `owner`, which is pushed into SQL as the
// authenticated actor in addition to the mandatory workspace.
type JobAccess struct {
	PermissionKey string
	WorkspaceID   string
	OwnerActorID  string
}

func (access JobAccess) Normalized() JobAccess {
	access.PermissionKey = strings.TrimSpace(access.PermissionKey)
	access.WorkspaceID = strings.TrimSpace(access.WorkspaceID)
	access.OwnerActorID = strings.TrimSpace(access.OwnerActorID)
	return access
}

type FailurePlan struct {
	Status        string
	Code          string
	Attempts      int
	NextAttemptAt time.Time
}
