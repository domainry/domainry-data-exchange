// Package dataexchangesdkadapter adapts application use cases to the public SDK.
package dataexchangesdkadapter

import (
	"context"
	"fmt"

	dataexchange "github.com/domainry/domainry-data-exchange-sdk"
	"github.com/domainry/domainry-data-exchange-sdk/modulehost"
	dataexchangeapplication "github.com/domainry/domainry-data-exchange/internal/application/dataexchange"
	dataexchangerepository "github.com/domainry/domainry-data-exchange/internal/domain/dataexchange/repository"
	"github.com/domainry/domainry-foundation/modulecapability"
	"github.com/domainry/domainry-foundation/modulehttp"
)

// Binding deliberately embeds the application service: the adapter owns the
// SDK-facing type while orchestration remains in the application layer.
type Binding struct {
	*dataexchangeapplication.Service
	surfaces   []modulehttp.Surface
	capability modulecapability.Binding
}

func NewBinding(application dataexchange.ApplicationRef, host modulehost.Host, store dataexchangerepository.JobRepository, capabilities ...modulecapability.Binding) *Binding {
	var capability modulecapability.Binding
	if len(capabilities) != 0 {
		capability = capabilities[0]
	}
	return &Binding{Service: dataexchangeapplication.NewService(application, host, store), capability: capability}
}

func (b *Binding) CapabilitySummary(ctx context.Context) (modulecapability.ModuleSummary, error) {
	if b.capability == nil {
		return modulecapability.ModuleSummary{}, fmt.Errorf("Data Exchange capability binding is unavailable")
	}
	return b.capability.CapabilitySummary(ctx)
}
func (b *Binding) CapabilityCategory(ctx context.Context, key string) (modulecapability.CategoryDocument, error) {
	if b.capability == nil {
		return modulecapability.CategoryDocument{}, fmt.Errorf("Data Exchange capability binding is unavailable")
	}
	return b.capability.CapabilityCategory(ctx, key)
}
func (b *Binding) ValidateCapabilityCandidate(ctx context.Context, request modulecapability.ValidationRequest) (modulecapability.ValidationResult, error) {
	if b.capability == nil {
		return modulecapability.ValidationResult{}, fmt.Errorf("Data Exchange capability binding is unavailable")
	}
	return b.capability.ValidateCapabilityCandidate(ctx, request)
}

func (b *Binding) SetHTTPSurfaces(surfaces []modulehttp.Surface) {
	b.surfaces = append([]modulehttp.Surface(nil), surfaces...)
}

func (b *Binding) HTTPSurfaces() []modulehttp.Surface {
	return append([]modulehttp.Surface(nil), b.surfaces...)
}

var _ dataexchange.Binding = (*Binding)(nil)
var _ modulehttp.Provider = (*Binding)(nil)
