// Package capability exposes Data Exchange's source-owned capability contract
// without opening persistence or workers.
package capability

import (
	"github.com/domainry/domainry-data-exchange-sdk/modulehost"
	internalcapability "github.com/domainry/domainry-data-exchange/internal/capability"
	"github.com/domainry/domainry-foundation/modulecapability"
)

type Inputs struct {
	Host modulehost.Host
}

func Open(inputs Inputs) (*modulecapability.StaticBinding, error) {
	return internalcapability.NewBinding(inputs.Host)
}
