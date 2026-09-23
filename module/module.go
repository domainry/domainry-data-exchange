// Package module is the public facade for the in-process Data Exchange module.
// Implementation details remain under internal/assembly.
package module

import (
	moduleassembly "github.com/domainry/domainry-data-exchange/internal/assembly/module"
	persistenceschema "github.com/domainry/domainry-data-exchange/internal/infrastructure/persistence/database/schema"
	"github.com/domainry/domainry-foundation/schemaownership"
)

type Options = moduleassembly.Options
type Factory = moduleassembly.Factory

var NewFactory = moduleassembly.NewFactory

// SchemaOwnership returns only Data Exchange-owned physical tables. Shared
// Foundation tables are registered by their source-owning packages.
func SchemaOwnership() []schemaownership.Table { return persistenceschema.SchemaOwnership() }

func OwnedTables() []string { return schemaownership.Names(SchemaOwnership()) }
