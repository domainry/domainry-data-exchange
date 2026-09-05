package capability

import (
	"encoding/json"
	"testing"

	"github.com/domainry/domainry-foundation/modulecapability"
	"github.com/domainry/domainry-foundation/modulecapability/contracttest"
)

func TestDataExchangeCapabilityTracksJobsAndTransferContractsWithoutRuntimeRequestValidation(t *testing.T) {
	binding, err := NewBinding(nil)
	if err != nil {
		t.Fatal(err)
	}
	contracttest.VerifyBinding(t, binding)
	summary, err := binding.CapabilitySummary(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	contracttest.VerifyModuleRemoteParity(t, binding, contracttest.ValidationCase{
		Name: "undeclared validation scope fails identically",
		Request: modulecapability.ValidationRequest{
			ContractVersion: modulecapability.ValidationContractVersion, ModuleKey: summary.Identity.Key,
			CategoryKey: TransferCategory, ContractSHA256: summary.Identity.ContractSHA256,
			Kind:      "data_exchange.validation_schema",
			Candidate: modulecapability.AuthoringFragment{Collection: "model.fields", Key: "import", Value: json.RawMessage(`{}`)},
		},
	})
	operations, projections := 0, 0
	for _, category := range summary.Categories {
		operations += category.OperationCount
		projections += category.ProjectionCount
		if len(category.ValidationScopes) != 0 {
			t.Fatalf("Data Exchange runtime request DTOs leaked into model validation scopes: %v", category.ValidationScopes)
		}
	}
	if operations != 4 || projections != 2 {
		t.Fatalf("Data Exchange operations=%d projections=%d", operations, projections)
	}
}
