package saasassembly

import (
	"context"
	"encoding/json"
	"fmt"

	sdk "github.com/domainry/domainry-data-exchange-sdk"
	"github.com/domainry/domainry-data-exchange-sdk/saashost"
)

func (b *binding) SubjectLifecycle() sdk.SubjectLifecycle { return b }
func (b *binding) subjectTransport() (saashost.SubjectLifecycleTransport, error) {
	transport, ok := b.transport.(saashost.SubjectLifecycleTransport)
	if !ok {
		return nil, fmt.Errorf("Data Exchange SaaS subject lifecycle transport unavailable")
	}
	return transport, nil
}
func (b *binding) PreviewSubject(ctx context.Context, workspace, subject string) (json.RawMessage, error) {
	transport, err := b.subjectTransport()
	if err != nil {
		return nil, err
	}
	return transport.PreviewSubject(ctx, b.application, workspace, subject)
}
func (b *binding) ExportSubject(ctx context.Context, workspace, subject string) (json.RawMessage, error) {
	transport, err := b.subjectTransport()
	if err != nil {
		return nil, err
	}
	return transport.ExportSubject(ctx, b.application, workspace, subject)
}
func (b *binding) PrepareSubjectErasure(ctx context.Context, request sdk.SubjectErasureRequest) (json.RawMessage, error) {
	transport, err := b.subjectTransport()
	if err != nil {
		return nil, err
	}
	return transport.PrepareSubjectErasure(ctx, b.application, request)
}
func (b *binding) ErasePreparedSubject(ctx context.Context, request sdk.SubjectErasureRequest, plan json.RawMessage) (json.RawMessage, error) {
	transport, err := b.subjectTransport()
	if err != nil {
		return nil, err
	}
	return transport.ErasePreparedSubject(ctx, b.application, request, plan)
}

var _ sdk.SubjectLifecycle = (*binding)(nil)
var _ sdk.SubjectLifecycleBinding = (*binding)(nil)
