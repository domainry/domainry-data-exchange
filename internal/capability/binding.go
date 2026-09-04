package capability

import (
	"encoding/json"
	"fmt"

	dataexchange "github.com/domainry/domainry-data-exchange-sdk"
	"github.com/domainry/domainry-data-exchange-sdk/modulehost"
	modulehttptransport "github.com/domainry/domainry-data-exchange/internal/transport/http/module"
	"github.com/domainry/domainry-foundation/modulecapability"
)

const (
	JobsCategory     = "data_exchange.jobs"
	TransferCategory = "data_exchange.transfer"
)

type ImportCandidate struct {
	Provider       string `json:"provider"`
	ObjectKey      string `json:"object_key"`
	IdempotencyKey string `json:"idempotency_key"`
	Filename       string `json:"filename,omitempty"`
	ContentType    string `json:"content_type,omitempty"`
	MaxBytes       int64  `json:"max_bytes,omitempty"`
}

type ExportCandidate struct {
	Provider       string `json:"provider"`
	ObjectKey      string `json:"object_key"`
	IdempotencyKey string `json:"idempotency_key"`
	ReferenceID    string `json:"reference_id,omitempty"`
}

type JobSelectorCandidate struct {
	JobID     string `json:"job_id"`
	Provider  string `json:"provider,omitempty"`
	Operation string `json:"operation,omitempty"`
}

func NewBinding(_ modulehost.Host) (*modulecapability.StaticBinding, error) {
	contract := dataexchange.DataExchangeHTTPAdapterContract()
	routes, err := modulehttptransport.CapabilityRoutes()
	if err != nil {
		return nil, fmt.Errorf("project Data Exchange capability routes: %w", err)
	}
	overrides := map[string]modulecapability.OperationExtension{}
	for _, route := range routes {
		idempotency := modulecapability.Idempotency{Mode: route.Action.IdempotencyDecision}
		if route.Action.Key == dataexchange.ActionDataExchangeJobCancel {
			idempotency.KeySource = "path.jobID"
		}
		overrides[route.Pattern()] = modulecapability.OperationExtension{
			Owner: "data_exchange", Authorization: modulecapability.Authorization{
				Strategy: route.Action.Authorization.Strategy, PolicyKey: route.Action.Authorization.PolicyKey,
				Audiences: append([]string(nil), route.Action.Authorization.Audiences...), WorkspaceScope: "authenticated_workspace",
			},
			Effect: modulecapability.EffectClass(route.Action.EffectClass), Idempotency: idempotency,
		}
	}
	jobs, err := modulecapability.CategoryFromHTTPRoutes(modulecapability.HTTPRouteCategory{
		Owner: "data_exchange", Category: modulecapability.CategorySummary{
			Key: JobsCategory, Name: "Data Exchange jobs", Description: "Inspect, cancel, and download only the authenticated actor's workspace-isolated durable import/export jobs with the current exact Permission and its owner data scope.",
			AssemblyChains: []string{"data_exchange_job_to_file_download"}, ValidationScopes: []string{},
		},
		Routes: routes, Operations: contract.OpenAPIOperations(), WorkspaceScope: "authenticated_workspace", ExtensionOverrides: overrides,
		Components: map[string]map[string]json.RawMessage{
			"securitySchemes": {"BearerAuth": json.RawMessage(`{"type":"http","scheme":"bearer","bearerFormat":"JWT"}`)},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("project Data Exchange job capability: %w", err)
	}
	importSchema, err := json.Marshal(modulecapability.JSONSchemaForGoValue(ImportCandidate{}))
	if err != nil {
		return nil, err
	}
	exportSchema, err := json.Marshal(modulecapability.JSONSchemaForGoValue(ExportCandidate{}))
	if err != nil {
		return nil, err
	}
	transfer := modulecapability.CategoryDocument{
		Category: modulecapability.CategorySummary{
			Key: TransferCategory, Name: "Batch transfer", Description: "Validate common import/export job envelopes before a Records, Report, or other source owner supplies provider-specific options and content.",
			AssemblyChains: []string{"file_upload_to_data_exchange_import", "record_or_report_query_to_data_exchange_export"}, ValidationScopes: []string{},
		},
		OpenAPI: modulecapability.OpenAPIFragment{OpenAPI: "3.1.0", Paths: map[string]map[string]json.RawMessage{}},
		Projections: []modulecapability.SourceProjection{
			{Kind: "data_exchange.validation_schema", Key: "data_exchange.export", Payload: exportSchema},
			{Kind: "data_exchange.validation_schema", Key: "data_exchange.import", Payload: importSchema},
		},
	}
	summary := modulecapability.ModuleSummary{
		Identity: modulecapability.ModuleIdentity{
			Key: "data_exchange", SourceOwner: "data_exchange", ModuleVersion: dataexchange.ProtocolVersionV1, ValidationRevision: "data-exchange-request-validation-v1",
			SupportedDeploymentModes: []modulecapability.DeploymentMode{modulecapability.DeploymentModeModule, modulecapability.DeploymentModeSaaS},
		},
		Name: "Data Exchange", Description: "Owns durable streaming imports, paged or canonical exports, transfer jobs, chunks, integrity evidence, cancellation, and artifact lifecycle while business providers own row and artifact semantics.",
		Scenarios: modulecapability.AdaptationScenarios{
			UseWhen:              []string{"A PRD needs large or asynchronous data import/export, upload-backed processing, downloadable artifacts, progress tracking, cancellation, retries, or durable transfer evidence"},
			DoNotUseWhen:         []string{"The requirement is a small synchronous CRUD request, an on-screen report with no artifact, or direct file storage without import/export job semantics"},
			RequirementSignals:   []string{"CSV import", "bulk import", "export file", "download artifact", "transfer progress", "cancel export", "large dataset", "batch data"},
			ProvidedCapabilities: []string{"data_exchange.streaming_import", "data_exchange.paged_export", "data_exchange.canonical_artifact", "data_exchange.durable_job", "data_exchange.job_cancel", "data_exchange.artifact_download", "data_exchange.integrity_evidence"},
			RequiredModules:      []string{"identity"}, OptionalModules: []string{"audit", "report"}, ConflictingModules: []string{},
			AssemblyChains:    []string{"file_upload_to_data_exchange_import", "record_or_report_query_to_data_exchange_export", "data_exchange_job_to_file_download"},
			ValidationScopes:  []string{},
			SelectionExamples: []modulecapability.ScenarioExample{{Requirement: "Import a large CSV asynchronously and expose progress, cancellation, and rejected-row evidence", Reason: "Data Exchange owns durable source chunks, worker recovery, transfer progress, and job lifecycle while the Records provider validates rows"}},
			RejectionExamples: []modulecapability.ScenarioExample{{Requirement: "Display a paginated report table in the browser", Reason: "Report query is sufficient until a durable downloadable export artifact is required"}},
		},
	}
	return modulecapability.NewStaticBinding(summary, []modulecapability.CategoryDocument{jobs, transfer}, nil)
}
