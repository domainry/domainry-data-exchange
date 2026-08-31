// Package remote is the compatibility facade for the Data Exchange SaaS client.
// SaaS assembly remains internal so deployment code does not leak through the API.
package remote

import (
	"github.com/domainry/domainry-data-exchange-sdk/saashost"
	saasassembly "github.com/domainry/domainry-data-exchange/internal/assembly/saas"
)

type Factory = saasassembly.Factory

func NewFactory(transport saashost.Transport) Factory { return saasassembly.NewFactory(transport) }
