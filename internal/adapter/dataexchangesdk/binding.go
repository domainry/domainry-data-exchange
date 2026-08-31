// Package dataexchangesdkadapter adapts application use cases to the public SDK.
package dataexchangesdkadapter

import (
	dataexchange "github.com/domainry/domainry-data-exchange-sdk"
	"github.com/domainry/domainry-data-exchange-sdk/modulehost"
	dataexchangeapplication "github.com/domainry/domainry-data-exchange/internal/application/dataexchange"
	persistence "github.com/domainry/domainry-data-exchange/internal/infrastructure/persistence/database/dataexchange"
)

// Binding deliberately embeds the application service: the adapter owns the
// SDK-facing type while orchestration remains in the application layer.
type Binding struct {
	*dataexchangeapplication.Service
}

func NewBinding(application dataexchange.ApplicationRef, host modulehost.Host, store *persistence.Store) *Binding {
	return &Binding{Service: dataexchangeapplication.NewService(application, host, store)}
}

var _ dataexchange.Binding = (*Binding)(nil)
