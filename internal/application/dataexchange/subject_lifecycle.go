package dataexchangeapplication

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	sdk "github.com/domainry/domainry-data-exchange-sdk"
	repository "github.com/domainry/domainry-data-exchange/internal/domain/dataexchange/repository"
	"github.com/domainry/domainry-foundation/requestcontext"
)

func (b *Service) SubjectLifecycle() sdk.SubjectLifecycle { return b }

func (b *Service) subjectLifecycleRepository(ctx context.Context, workspace, subject string) (repository.SubjectLifecycleRepository, error) {
	if strings.TrimSpace(workspace) == "" || workspace != requestcontext.WorkspaceID(ctx) || strings.TrimSpace(subject) == "" {
		return nil, fmt.Errorf("Data Exchange subject lifecycle scope mismatch")
	}
	store, ok := b.store.(repository.SubjectLifecycleRepository)
	if !ok {
		return nil, fmt.Errorf("Data Exchange subject lifecycle unavailable")
	}
	return store, nil
}

func validateSubjectErasure(request sdk.SubjectErasureRequest) error {
	if strings.TrimSpace(request.RequestID) == "" {
		return fmt.Errorf("Data Exchange erasure request required")
	}
	var holds []json.RawMessage
	if len(request.LegalHolds) > 0 && json.Unmarshal(request.LegalHolds, &holds) != nil {
		return fmt.Errorf("Data Exchange legal holds invalid")
	}
	if len(holds) > 0 {
		return fmt.Errorf("Data Exchange erasure blocked by legal hold")
	}
	return nil
}

func (b *Service) PreviewSubject(ctx context.Context, workspace, subject string) (json.RawMessage, error) {
	store, err := b.subjectLifecycleRepository(ctx, workspace, subject)
	if err != nil {
		return nil, err
	}
	return store.PreviewSubject(ctx, workspace, subject)
}

func (b *Service) ExportSubject(ctx context.Context, workspace, subject string) (json.RawMessage, error) {
	// Job identity and state are exported without copying raw import/export PII.
	return b.PreviewSubject(ctx, workspace, subject)
}

func (b *Service) PrepareSubjectErasure(ctx context.Context, request sdk.SubjectErasureRequest) (json.RawMessage, error) {
	if err := validateSubjectErasure(request); err != nil {
		return nil, err
	}
	store, err := b.subjectLifecycleRepository(ctx, request.WorkspaceID, request.SubjectID)
	if err != nil {
		return nil, err
	}
	return store.PrepareSubjectErasure(ctx, request)
}

func (b *Service) ErasePreparedSubject(ctx context.Context, request sdk.SubjectErasureRequest, raw json.RawMessage) (json.RawMessage, error) {
	if err := validateSubjectErasure(request); err != nil {
		return nil, err
	}
	store, err := b.subjectLifecycleRepository(ctx, request.WorkspaceID, request.SubjectID)
	if err != nil {
		return nil, err
	}
	return store.ErasePreparedSubject(ctx, request, raw)
}

var _ sdk.SubjectLifecycle = (*Service)(nil)
