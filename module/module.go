// Package module is the public facade for the in-process Data Exchange module.
// Implementation details remain under internal/assembly.
package module

import (
	"github.com/domainry/domainry-data-exchange-sdk/modulehost"
	"github.com/domainry/domainry-data-exchange-sdk/saashost"
	moduleassembly "github.com/domainry/domainry-data-exchange/internal/assembly/module"
	saasassembly "github.com/domainry/domainry-data-exchange/internal/assembly/saas"
	persistenceengine "github.com/domainry/domainry-data-exchange/internal/infrastructure/persistence"
	persistenceschema "github.com/domainry/domainry-data-exchange/internal/infrastructure/persistence/database/schema"
	"github.com/domainry/domainry-foundation/schemaownership"
)

type Options = moduleassembly.Options
type Factory = moduleassembly.Factory
type SaaSFactory = saasassembly.Factory

var NewFactory = moduleassembly.NewFactory

func NewSaaSFactory(transport saashost.Transport) SaaSFactory {
	return saasassembly.NewFactory(transport)
}

// SchemaOwnership returns only Data Exchange-owned physical tables. Shared
// Foundation tables are registered by their source-owning packages.
func SchemaOwnership() []schemaownership.Table { return persistenceschema.SchemaOwnership() }

func OwnedTables() []string { return schemaownership.Names(SchemaOwnership()) }

// SchemaMigrations exposes the same source-owned DDL used by Module startup for
// cross-module composition verification without exposing a persistence Store.
func SchemaMigrations(driver, schema string) ([]modulehost.Migration, error) {
	engine, err := persistenceengine.NewEngine(driver)
	if err != nil {
		return nil, err
	}
	return persistenceschema.SchemaMigrations(engine, schema)
}
