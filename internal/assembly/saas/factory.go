package saasassembly

import (
	"context"
	"fmt"
	"sync"

	dataexchange "github.com/domainry/domainry-data-exchange-sdk"
	"github.com/domainry/domainry-data-exchange-sdk/modulehost"
	"github.com/domainry/domainry-data-exchange-sdk/saashost"
	sourcecapability "github.com/domainry/domainry-data-exchange/capability"
	modulehttptransport "github.com/domainry/domainry-data-exchange/internal/transport/http/module"
	"github.com/domainry/domainry-foundation/modulecapability"
	"github.com/domainry/domainry-foundation/modulehttp"
)

type Factory struct{ transport saashost.Transport }

func NewFactory(transport saashost.Transport) Factory { return Factory{transport: transport} }
func (Factory) Open(context.Context, dataexchange.ApplicationRef) (dataexchange.Binding, error) {
	return nil, fmt.Errorf("Data Exchange SaaS requires Runtime host capabilities")
}
func (f Factory) OpenSaaS(ctx context.Context, app dataexchange.ApplicationRef, host modulehost.Host) (dataexchange.Binding, error) {
	if err := app.Validate(); err != nil {
		return nil, err
	}
	if f.transport == nil || host == nil {
		return nil, fmt.Errorf("Data Exchange SaaS transport and host are required")
	}
	localCapability, err := sourcecapability.Open(sourcecapability.Inputs{})
	if err != nil {
		return nil, fmt.Errorf("open Data Exchange source capability: %w", err)
	}
	localSummary, err := localCapability.CapabilitySummary(ctx)
	if err != nil {
		return nil, fmt.Errorf("load Data Exchange source capability summary: %w", err)
	}
	if err := modulecapability.VerifyPinnedBinding(ctx, f.transport, "data_exchange", localSummary.Identity.ContractSHA256); err != nil {
		return nil, fmt.Errorf("verify Data Exchange SaaS capability: %w", err)
	}
	descriptor, err := f.transport.Descriptor(ctx, app)
	if err != nil {
		return nil, err
	}
	if err := descriptor.Validate(); err != nil || descriptor.Mode != dataexchange.DeploymentModeSaaS {
		return nil, fmt.Errorf("Data Exchange SaaS descriptor is incompatible")
	}
	if err := f.transport.Connect(ctx, app, host); err != nil {
		return nil, fmt.Errorf("connect Data Exchange SaaS provider bridge: %w", err)
	}
	result := &binding{application: app, transport: f.transport, descriptor: descriptor}
	adapter, err := modulehttptransport.NewAdapter(result, host)
	if err != nil {
		_ = f.transport.Close(context.WithoutCancel(ctx), app)
		return nil, err
	}
	result.adapters = []modulehttp.Adapter{adapter}
	return result, nil
}

type binding struct {
	application dataexchange.ApplicationRef
	transport   saashost.Transport
	descriptor  dataexchange.Descriptor
	adapters    []modulehttp.Adapter
	closeOnce   sync.Once
}

func (b *binding) Descriptor() dataexchange.Descriptor { return b.descriptor }
func (b *binding) CapabilitySummary(ctx context.Context) (modulecapability.ModuleSummary, error) {
	return b.transport.CapabilitySummary(ctx)
}
func (b *binding) CapabilityCategory(ctx context.Context, key string) (modulecapability.CategoryDocument, error) {
	return b.transport.CapabilityCategory(ctx, key)
}
func (b *binding) ValidateCapabilityCandidate(ctx context.Context, request modulecapability.ValidationRequest) (modulecapability.ValidationResult, error) {
	return b.transport.ValidateCapabilityCandidate(ctx, request)
}
func (b *binding) HTTPAdapters() []modulehttp.Adapter {
	return append([]modulehttp.Adapter(nil), b.adapters...)
}
func (b *binding) SubmitImport(ctx context.Context, r dataexchange.ImportRequest) (dataexchange.Job, bool, error) {
	return b.transport.SubmitImport(ctx, b.application, r)
}
func (b *binding) SubmitExport(ctx context.Context, r dataexchange.ExportRequest) (dataexchange.Job, bool, error) {
	return b.transport.SubmitExport(ctx, b.application, r)
}
func (b *binding) Job(ctx context.Context, r dataexchange.JobRequest) (dataexchange.Job, error) {
	return b.transport.Job(ctx, b.application, r)
}
func (b *binding) Cancel(ctx context.Context, r dataexchange.JobRequest) (dataexchange.Job, error) {
	return b.transport.Cancel(ctx, b.application, r)
}
func (b *binding) Download(ctx context.Context, r dataexchange.JobRequest) (dataexchange.Artifact, error) {
	return b.transport.Download(ctx, b.application, r)
}
func (*binding) Start(context.Context, dataexchange.WorkerConfig) <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
func (b *binding) Close(ctx context.Context) error {
	var err error
	b.closeOnce.Do(func() { err = b.transport.Close(ctx, b.application) })
	return err
}

var _ dataexchange.Binding = (*binding)(nil)
var _ modulehttp.Provider = (*binding)(nil)
var _ dataexchange.Factory = Factory{}
var _ saashost.Factory = Factory{}
