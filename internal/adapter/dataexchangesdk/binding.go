// Package dataexchangesdkadapter adapts application use cases to the public SDK.
package dataexchangesdkadapter

import (
	dataexchange "github.com/domainry/domainry-data-exchange-sdk"
	"github.com/domainry/domainry-data-exchange-sdk/modulehost"
	dataexchangeapplication "github.com/domainry/domainry-data-exchange/internal/application/dataexchange"
	dataexchangerepository "github.com/domainry/domainry-data-exchange/internal/domain/dataexchange/repository"
	"github.com/domainry/domainry-foundation/modulehttp"
)

// Binding deliberately embeds the application service: the adapter owns the
// SDK-facing type while orchestration remains in the application layer.
type Binding struct {
	*dataexchangeapplication.Service
	surfaces []modulehttp.Surface
}

func NewBinding(application dataexchange.ApplicationRef, host modulehost.Host, store dataexchangerepository.JobRepository) *Binding {
	return &Binding{Service: dataexchangeapplication.NewService(application, host, store)}
}

func (b *Binding) SetHTTPSurfaces(surfaces []modulehttp.Surface) {
	b.surfaces = append([]modulehttp.Surface(nil), surfaces...)
}

func (b *Binding) HTTPSurfaces() []modulehttp.Surface {
	return append([]modulehttp.Surface(nil), b.surfaces...)
}

var _ dataexchange.Binding = (*Binding)(nil)
var _ modulehttp.Provider = (*Binding)(nil)
