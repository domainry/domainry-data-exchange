// Package module is the public facade for the in-process Data Exchange module.
// Implementation details remain under internal/assembly.
package module

import (
	moduleassembly "github.com/domainry/domainry-data-exchange/internal/assembly/module"
)

type Options = moduleassembly.Options
type Factory = moduleassembly.Factory

var NewFactory = moduleassembly.NewFactory
