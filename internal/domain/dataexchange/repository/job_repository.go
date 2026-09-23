// Package repository defines the persistence boundary owned by Data Exchange.
package repository

import (
	"context"
	"encoding/json"
	"io"
	"time"

	dataexchange "github.com/domainry/domainry-data-exchange-sdk"
	"github.com/domainry/domainry-data-exchange/internal/domain/dataexchange/model"
)

type JobRepository interface {
	SubmitImport(context.Context, dataexchange.ImportRequest) (dataexchange.Job, bool, error)
	SubmitExport(context.Context, dataexchange.ExportRequest) (dataexchange.Job, bool, error)
	Jobs(context.Context, dataexchange.JobListRequest, model.JobAccess) ([]dataexchange.Job, error)
	Job(context.Context, dataexchange.JobRequest, model.JobAccess) (dataexchange.Job, error)
	Cancel(context.Context, dataexchange.JobRequest, model.JobAccess) (dataexchange.Job, error)
	Artifact(context.Context, dataexchange.JobRequest, model.JobAccess) (dataexchange.Artifact, error)
	Claim(context.Context, string, time.Duration) (model.WorkItem, bool, error)
	ClaimJob(context.Context, dataexchange.JobRequest, string, time.Duration) (model.WorkItem, bool, error)
	Chunks(context.Context, string, string, string) (io.ReadCloser, error)
	ResultIdentity(context.Context, string, string) (string, int64, error)
	CommitResultPage(context.Context, model.WorkItem, int, []byte, string, int, int) error
	Progress(context.Context, model.WorkItem, int, int) error
	Complete(context.Context, model.WorkItem, *model.ArtifactRecord) error
	Fail(context.Context, model.WorkItem, model.FailurePlan) error
	Heartbeat(context.Context, model.WorkItem, time.Duration) error
	Scoped(context.Context, string, string) context.Context
}

// SubjectLifecycleRepository keeps only Data Exchange cleanup behavior. The
// host Lifecycle owner persists fences, frozen plans and owner results.
type SubjectLifecycleRepository interface {
	PreviewSubject(context.Context, string, string) (json.RawMessage, error)
	PrepareSubjectErasure(context.Context, dataexchange.SubjectErasureRequest) (json.RawMessage, error)
	ErasePreparedSubject(context.Context, dataexchange.SubjectErasureRequest, json.RawMessage) (json.RawMessage, error)
}

type SubjectLifecyclePersistenceBinder interface {
	BindSubjectLifecyclePersistence()
	SubjectLifecyclePersistenceBound() bool
}
