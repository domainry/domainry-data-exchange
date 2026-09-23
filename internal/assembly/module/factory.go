package moduleassembly

import (
	"context"
	"fmt"

	dataexchange "github.com/domainry/domainry-data-exchange-sdk"
	"github.com/domainry/domainry-data-exchange-sdk/modulehost"
	dataexchangesdkadapter "github.com/domainry/domainry-data-exchange/internal/adapter/dataexchangesdk"
	persistenceengine "github.com/domainry/domainry-data-exchange/internal/infrastructure/persistence"
	persistence "github.com/domainry/domainry-data-exchange/internal/infrastructure/persistence/database/dataexchange"
	persistenceschema "github.com/domainry/domainry-data-exchange/internal/infrastructure/persistence/database/schema"
	modulehttptransport "github.com/domainry/domainry-data-exchange/internal/transport/http/module"
	sharedartifact "github.com/domainry/domainry-foundation/artifact"
	"github.com/domainry/domainry-foundation/modulehttp"
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
	engine, err := persistenceengine.NewEngine(host.Migrations().Driver())
	if err != nil {
		return nil, err
	}
	migrations, err := persistenceschema.SchemaMigrations(engine, host.Migrations().Schema())
	if err != nil {
		return nil, err
	}
	if err := host.Migrations().ApplyOwnedMigrations(ctx, "data_exchange", migrations); err != nil {
		return nil, fmt.Errorf("apply Data Exchange Module migrations: %w", err)
	}
	renderer := engine.Dialect().WithSchema(host.Migrations().Schema())
	artifacts, err := sharedartifact.Open(ctx, host.Database(), renderer, artifactMigrationRegistrar{target: host.Migrations()})
	if err != nil {
		return nil, fmt.Errorf("open Data Exchange Artifact persistence: %w", err)
	}
	store, err := persistence.NewStore(host.Database(), engine, host.Migrations().Schema(), artifacts, host.WorkspaceContext)
	if err != nil {
		return nil, err
	}
	binding := dataexchangesdkadapter.NewBinding(application, host, store)
	adapter, err := modulehttptransport.NewAdapter(binding, host)
	if err != nil {
		return nil, err
	}
	binding.SetHTTPAdapters([]modulehttp.Adapter{adapter})
	return binding, nil
}

type artifactMigrationRegistrar struct{ target modulehost.MigrationRegistrar }

func (r artifactMigrationRegistrar) ApplyOwnedMigrations(ctx context.Context, owner string, migrations []sharedartifact.SchemaMigration) error {
	items := make([]modulehost.Migration, 0)
	for _, migration := range migrations {
		for index, statement := range migration.Statements {
			items = append(items, modulehost.Migration{
				ID:  fmt.Sprintf("artifact_v%d_%03d", migration.Version, index+1),
				SQL: statement,
			})
		}
	}
	return r.target.ApplyOwnedMigrations(ctx, owner, items)
}

var _ dataexchange.Factory = (*Factory)(nil)
var _ modulehost.Factory = (*Factory)(nil)
