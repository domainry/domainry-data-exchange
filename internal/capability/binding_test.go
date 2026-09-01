package capability

import (
	"testing"

	"github.com/domainry/domainry-foundation/modulecapability/contracttest"
)

func TestDataExchangeCapabilityTracksJobsAndTransferContractsWithoutRuntimeRequestValidation(t *testing.T) {
	binding, err := NewBinding(nil)
	if err != nil {
		t.Fatal(err)
	}
	contracttest.VerifyBinding(t, binding)
	contracttest.VerifyModuleRemoteParity(t, binding)
	summary, err := binding.CapabilitySummary(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	operations, projections := 0, 0
	for _, category := range summary.Categories {
		operations += category.OperationCount
		projections += category.ProjectionCount
		if len(category.ValidationScopes) != 0 {
			t.Fatalf("Data Exchange runtime request DTOs leaked into model validation scopes: %v", category.ValidationScopes)
		}
	}
	if operations != 3 || projections != 2 {
		t.Fatalf("Data Exchange operations=%d projections=%d", operations, projections)
	}
}
