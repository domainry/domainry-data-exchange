// Package repository defines the persistence boundary owned by Data Exchange.
package repository

import (
	"context"
	"io"
	"time"

	dataexchange "github.com/domainry/domainry-data-exchange-sdk"
	"github.com/domainry/domainry-data-exchange/internal/domain/dataexchange/model"
)

type JobRepository interface {
	SubmitImport(context.Context, dataexchange.ImportRequest) (dataexchange.Job, bool, error)
	SubmitExport(context.Context, dataexchange.ExportRequest) (dataexchange.Job, bool, error)
	Job(context.Context, dataexchange.JobRequest) (dataexchange.Job, error)
	Cancel(context.Context, dataexchange.JobRequest) (dataexchange.Job, error)
	Artifact(context.Context, dataexchange.JobRequest) (dataexchange.Artifact, error)
	Claim(context.Context, string, time.Duration) (model.WorkItem, bool, error)
	Chunks(context.Context, string, string, string) (io.ReadCloser, error)
	ResultIdentity(context.Context, string, string) (string, int64, error)
	CommitResultPage(context.Context, model.WorkItem, int, []byte, string, int, int) error
	Progress(context.Context, model.WorkItem, int, int) error
	Complete(context.Context, model.WorkItem, *model.ArtifactRecord) error
	Fail(context.Context, model.WorkItem, model.FailurePlan) error
	Heartbeat(context.Context, model.WorkItem, time.Duration) error
	Scoped(context.Context, string, string) context.Context
}
