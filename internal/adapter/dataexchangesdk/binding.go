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
	adapters []modulehttp.Adapter
}

func NewBinding(application dataexchange.ApplicationRef, host modulehost.Host, store dataexchangerepository.JobRepository) *Binding {
	return &Binding{Service: dataexchangeapplication.NewService(application, host, store)}
}

func (b *Binding) SetHTTPAdapters(adapters []modulehttp.Adapter) {
	b.adapters = append([]modulehttp.Adapter(nil), adapters...)
}

func (b *Binding) HTTPAdapters() []modulehttp.Adapter {
	return append([]modulehttp.Adapter(nil), b.adapters...)
}

var _ dataexchange.Binding = (*Binding)(nil)
var _ dataexchange.SubjectLifecyclePersistenceBinding = (*Binding)(nil)
var _ modulehttp.Provider = (*Binding)(nil)
