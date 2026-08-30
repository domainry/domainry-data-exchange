package module

import (
	"context"
	"fmt"

	dataexchange "github.com/domainry/domainry-data-exchange-sdk"
	"github.com/domainry/domainry-data-exchange-sdk/modulehost"
)

type Options struct{}

type Factory struct{ options Options }

func NewFactory(options Options) *Factory { return &Factory{options: options} }

func (*Factory) Open(context.Context, dataexchange.ApplicationRef) (dataexchange.Binding, error) {
	return nil, fmt.Errorf("Data Exchange Module host is required")
}

func (*Factory) OpenModule(ctx context.Context, application dataexchange.ApplicationRef, host modulehost.ModuleHost) (dataexchange.Binding, error) {
	if err := application.Validate(); err != nil {
		return nil, err
	}
	if host == nil || host.Database() == nil || host.Migrations() == nil {
		return nil, fmt.Errorf("Data Exchange Module host is incomplete")
	}
	migrations, err := schemaMigrations(host.Migrations().Driver(), host.Migrations().Schema())
	if err != nil {
		return nil, err
	}
	if err := host.Migrations().ApplyOwnedMigrations(ctx, "data_exchange", migrations); err != nil {
		return nil, fmt.Errorf("apply Data Exchange Module migrations: %w", err)
	}
	store := newSQLStore(host.Database(), host.Migrations().Driver(), host.Migrations().Schema(), host.WorkspaceContext)
	return newBinding(application, host, store), nil
}

var _ dataexchange.Factory = (*Factory)(nil)
var _ modulehost.Factory = (*Factory)(nil)
