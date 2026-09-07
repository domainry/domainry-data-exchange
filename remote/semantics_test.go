package remote

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	dataexchange "github.com/domainry/domainry-data-exchange-sdk"
	"github.com/domainry/domainry-data-exchange-sdk/modulehost"
	sourcecapability "github.com/domainry/domainry-data-exchange/capability"
	"github.com/domainry/domainry-foundation/apperror"
	"github.com/domainry/domainry-foundation/modulecapability"
	"github.com/domainry/domainry-foundation/modulehttp"
	identitysdk "github.com/domainry/domainry-identity-sdk"
)

type pageExportProvider struct{}

func (pageExportProvider) ReadExportPage(context.Context, dataexchange.ExportPageRequest) (dataexchange.ExportPage, error) {
	return dataexchange.ExportPage{}, nil
}

type fileExportProvider struct{ pageExportProvider }

func (fileExportProvider) OpenDataExchangeArtifact(context.Context, dataexchange.Job, dataexchange.Scope) (dataexchange.Artifact, error) {
	content := "provider-owned\n"
	return dataexchange.Artifact{
		Filename: "provider.csv", ContentType: "text/csv", Size: int64(len(content)), ExpiresAt: time.Now().UTC().Add(time.Hour),
		Content: io.NopCloser(strings.NewReader(content)),
	}, nil
}

type semanticHost struct{ exporter modulehost.ExportProvider }

func (semanticHost) ImportProvider(string) (modulehost.ImportProvider, bool) { return nil, false }
func (h semanticHost) ExportProvider(key string) (modulehost.ExportProvider, bool) {
	if key != "records" || h.exporter == nil {
		return nil, false
	}
	return h.exporter, true
}

type semanticTransport struct {
	modulecapability.Binding
	connected      modulehost.Host
	application    dataexchange.ApplicationRef
	lastJobRequest dataexchange.JobRequest
	lastImport     dataexchange.ImportRequest
	lastExport     dataexchange.ExportRequest
	job            dataexchange.Job
	artifact       dataexchange.Artifact
	jobErr         error
	cancelErr      error
	downloadErr    error
	importErr      error
	exportErr      error
	calls          []string
}

func (t *semanticTransport) Connect(_ context.Context, application dataexchange.ApplicationRef, host modulehost.Host) error {
	t.application = application
	t.connected = host
	return nil
}
func (*semanticTransport) Descriptor(context.Context, dataexchange.ApplicationRef) (dataexchange.Descriptor, error) {
	return dataexchange.Descriptor{ProtocolVersion: dataexchange.ProtocolVersionV1, Mode: dataexchange.DeploymentModeSaaS}, nil
}
func (t *semanticTransport) SubmitImport(_ context.Context, application dataexchange.ApplicationRef, request dataexchange.ImportRequest) (dataexchange.Job, bool, error) {
	t.application, t.lastImport = application, request
	t.calls = append(t.calls, "submit_import")
	if t.importErr != nil {
		return dataexchange.Job{}, false, t.importErr
	}
	return dataexchange.Job{ID: "remote-import", Status: "queued"}, false, nil
}
func (t *semanticTransport) SubmitExport(_ context.Context, application dataexchange.ApplicationRef, request dataexchange.ExportRequest) (dataexchange.Job, bool, error) {
	t.application, t.lastExport = application, request
	t.calls = append(t.calls, "submit_export")
	if t.exportErr != nil {
		return dataexchange.Job{}, false, t.exportErr
	}
	return dataexchange.Job{ID: "remote-export", Status: "queued"}, false, nil
}
func (t *semanticTransport) Job(_ context.Context, application dataexchange.ApplicationRef, request dataexchange.JobRequest) (dataexchange.Job, error) {
	t.application, t.lastJobRequest = application, request
	t.calls = append(t.calls, "job")
	return t.job, t.jobErr
}
func (t *semanticTransport) Cancel(_ context.Context, application dataexchange.ApplicationRef, request dataexchange.JobRequest) (dataexchange.Job, error) {
	t.application, t.lastJobRequest = application, request
	t.calls = append(t.calls, "cancel")
	if t.cancelErr != nil {
		return dataexchange.Job{}, t.cancelErr
	}
	job := t.job
	job.Status = "cancelled"
	return job, nil
}
func (t *semanticTransport) Download(_ context.Context, application dataexchange.ApplicationRef, request dataexchange.JobRequest) (dataexchange.Artifact, error) {
	t.application, t.lastJobRequest = application, request
	t.calls = append(t.calls, "download")
	return t.artifact, t.downloadErr
}
func (*semanticTransport) Close(context.Context, dataexchange.ApplicationRef) error { return nil }

func openSemanticBinding(t *testing.T, host semanticHost, configure func(*semanticTransport)) (dataexchange.Binding, *semanticTransport, modulecapability.Binding) {
	t.Helper()
	direct, err := sourcecapability.Open(sourcecapability.Inputs{Host: host})
	if err != nil {
		t.Fatal(err)
	}
	transport := &semanticTransport{Binding: direct}
	if configure != nil {
		configure(transport)
	}
	binding, err := NewFactory(transport).OpenSaaS(t.Context(), dataexchange.ApplicationRef{ApplicationID: "app", RuntimeID: "runtime"}, host)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = binding.Close(context.Background()) })
	return binding, transport, direct
}

func remoteRequest(method, target string, permission string) *http.Request {
	request := httptest.NewRequest(method, target, nil)
	separator := strings.LastIndexByte(permission, '.')
	resource, action := permission[:separator], permission[separator+1:]
	bundle := &identitysdk.AccessBundle{
		ContractVersion: identitysdk.CurrentPolicyBundleVersion,
		Subject:         identitysdk.Subject{WorkspaceID: "workspace", SubjectID: "actor"},
		FunctionGrants:  []identitysdk.FunctionGrant{{Resource: identitysdk.ResourceType(resource), Action: identitysdk.Action(action), Effect: identitysdk.EffectAllow}},
		DataPolicies: []identitysdk.DataPolicy{{
			Key: "data-" + permission + "-0", Resource: identitysdk.ResourceType(resource), Action: identitysdk.Action(action), Effect: identitysdk.EffectAllow,
			DataScopes: []identitysdk.DataScope{identitysdk.DataScopeOwner}, Predicate: identitysdk.Predicate{Fact: "owner_user_id", Operator: identitysdk.OperatorEqual, Value: "$subject.id"},
		}},
	}
	ctx := identitysdk.WithRequestIdentity(request.Context(), identitysdk.RequestIdentity{Principal: identitysdk.Principal{
		Known: true, WorkspaceID: "workspace", UserID: "actor", RoleKey: "member", AccessBundle: bundle,
	}})
	return request.WithContext(ctx)
}

func remoteAdapter(t *testing.T, binding dataexchange.Binding) modulehttp.Adapter {
	t.Helper()
	provider, ok := binding.(modulehttp.Provider)
	if !ok || len(provider.HTTPAdapters()) != 1 {
		t.Fatal("SaaS binding did not expose exactly one Data Exchange HTTP adapter")
	}
	return provider.HTTPAdapters()[0]
}

func TestSaaSSurfacePreservesRemoteSemanticsErrorsAndFileBoundary(t *testing.T) {
	completed := dataexchange.Job{
		ID: "job-1", Provider: "records", Operation: "export", Status: "completed", ObjectKey: "contact",
		WorkspaceID: "workspace", ActorID: "actor", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	t.Run("transport download", func(t *testing.T) {
		content := "remote-owned\n"
		binding, transport, _ := openSemanticBinding(t, semanticHost{exporter: pageExportProvider{}}, func(transport *semanticTransport) {
			transport.job = completed
			transport.artifact = dataexchange.Artifact{
				Filename: "remote.csv", ContentType: "text/csv", Size: int64(len(content)), ExpiresAt: time.Now().UTC().Add(time.Hour),
				Content: io.NopCloser(strings.NewReader(content)),
			}
		})
		response := httptest.NewRecorder()
		remoteAdapter(t, binding).Handler().ServeHTTP(response, remoteRequest(http.MethodGet, "/data-exchange/jobs/job-1/download?provider=records&operation=export", dataexchange.ActionDataExchangeJobDownload))
		if response.Code != http.StatusOK || response.Body.String() != content || response.Header().Get("Content-Disposition") != "attachment; filename=remote.csv" {
			t.Fatalf("status=%d headers=%v body=%q", response.Code, response.Header(), response.Body.String())
		}
		if !reflect.DeepEqual(transport.calls, []string{"job", "download"}) || transport.application.ApplicationID != "app" || transport.lastJobRequest.Provider != "records" || transport.lastJobRequest.Operation != "export" || transport.lastJobRequest.Scope.WorkspaceID != "workspace" {
			t.Fatalf("calls=%v application=%+v request=%+v", transport.calls, transport.application, transport.lastJobRequest)
		}
	})

	t.Run("provider-owned file delivery", func(t *testing.T) {
		binding, transport, _ := openSemanticBinding(t, semanticHost{exporter: fileExportProvider{}}, func(transport *semanticTransport) {
			transport.job = completed
			transport.artifact = dataexchange.Artifact{Content: io.NopCloser(strings.NewReader("must-not-be-read"))}
		})
		response := httptest.NewRecorder()
		remoteAdapter(t, binding).Handler().ServeHTTP(response, remoteRequest(http.MethodGet, "/data-exchange/jobs/job-1/download?provider=records&operation=export", dataexchange.ActionDataExchangeJobDownload))
		if response.Code != http.StatusOK || response.Body.String() != "provider-owned\n" || !reflect.DeepEqual(transport.calls, []string{"job"}) {
			t.Fatalf("status=%d body=%q transport_calls=%v", response.Code, response.Body.String(), transport.calls)
		}
	})

	for _, test := range []struct {
		name, method, target, permission, code string
		status                                 int
		configure                              func(*semanticTransport)
	}{
		{
			name: "not found", method: http.MethodGet, target: "/data-exchange/jobs/missing", permission: dataexchange.ActionDataExchangeJobGet,
			status: http.StatusNotFound, code: "backend.data_exchange.job_not_found",
			configure: func(transport *semanticTransport) { transport.jobErr = dataexchange.ErrJobNotFound },
		},
		{
			name: "remote conflict", method: http.MethodPost, target: "/data-exchange/jobs/job-1/cancel", permission: dataexchange.ActionDataExchangeJobCancel,
			status: http.StatusConflict, code: "backend.data_exchange.remote_conflict",
			configure: func(transport *semanticTransport) {
				transport.job = completed
				transport.cancelErr = &apperror.AppError{Kind: apperror.KindConflict, Code: "backend.data_exchange.remote_conflict"}
			},
		},
		{
			name: "expired artifact", method: http.MethodGet, target: "/data-exchange/jobs/job-1/download?provider=records&operation=export", permission: dataexchange.ActionDataExchangeJobDownload,
			status: http.StatusConflict, code: "backend.data_exchange.artifact_expired",
			configure: func(transport *semanticTransport) {
				transport.job = completed
				transport.downloadErr = dataexchange.ErrArtifactExpired
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			binding, _, _ := openSemanticBinding(t, semanticHost{exporter: pageExportProvider{}}, test.configure)
			response := httptest.NewRecorder()
			remoteAdapter(t, binding).Handler().ServeHTTP(response, remoteRequest(test.method, test.target, test.permission))
			if response.Code != test.status || !strings.Contains(response.Body.String(), `"code":"`+test.code+`"`) {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}
}

func TestSaaSBindingForwardsTransferEnvelopesWithoutBuffering(t *testing.T) {
	binding, transport, _ := openSemanticBinding(t, semanticHost{exporter: pageExportProvider{}}, nil)
	scope := dataexchange.Scope{WorkspaceID: "workspace", ActorID: "actor", RoleKey: "member", RequestID: "request-1"}
	source := strings.NewReader("id\n1\n")
	importRequest := dataexchange.ImportRequest{
		Scope: scope, Provider: "records", ObjectKey: "contact", IdempotencyKey: "import-1",
		Filename: "contacts.csv", ContentType: "text/csv", Options: []byte(`{"mode":"merge"}`), Source: source, MaxBytes: 1024,
	}
	job, replayed, err := binding.SubmitImport(t.Context(), importRequest)
	if err != nil || replayed || job.ID != "remote-import" {
		t.Fatalf("import job=%+v replayed=%v err=%v", job, replayed, err)
	}
	if transport.lastImport.Source != source || !reflect.DeepEqual(transport.lastImport.Scope, scope) || transport.lastImport.Provider != importRequest.Provider || transport.lastImport.ObjectKey != importRequest.ObjectKey || string(transport.lastImport.Options) != string(importRequest.Options) {
		t.Fatalf("forwarded import=%+v source_same=%v", transport.lastImport, transport.lastImport.Source == source)
	}

	exportRequest := dataexchange.ExportRequest{
		Scope: scope, Provider: "records", ObjectKey: "contact", IdempotencyKey: "export-1", ReferenceID: "audit-1", Options: []byte(`{"active":true}`),
	}
	job, replayed, err = binding.SubmitExport(t.Context(), exportRequest)
	if err != nil || replayed || job.ID != "remote-export" {
		t.Fatalf("export job=%+v replayed=%v err=%v", job, replayed, err)
	}
	if !reflect.DeepEqual(transport.lastExport, exportRequest) || !reflect.DeepEqual(transport.calls, []string{"submit_import", "submit_export"}) {
		t.Fatalf("forwarded export=%+v calls=%v", transport.lastExport, transport.calls)
	}
}

func TestSaaSBindingPreservesTypedIdempotencyConflictFromTransport(t *testing.T) {
	binding, transport, _ := openSemanticBinding(t, semanticHost{exporter: pageExportProvider{}}, func(transport *semanticTransport) {
		transport.importErr = fmt.Errorf("remote import: %w", dataexchange.ErrIdempotencyKeyReused)
		transport.exportErr = fmt.Errorf("remote export: %w", dataexchange.ErrIdempotencyKeyReused)
	})
	if _, _, err := binding.SubmitImport(t.Context(), dataexchange.ImportRequest{}); !errors.Is(err, dataexchange.ErrIdempotencyKeyReused) {
		t.Fatalf("import error=%v", err)
	}
	if _, _, err := binding.SubmitExport(t.Context(), dataexchange.ExportRequest{}); !errors.Is(err, dataexchange.ErrIdempotencyKeyReused) {
		t.Fatalf("export error=%v", err)
	}
	if !reflect.DeepEqual(transport.calls, []string{"submit_import", "submit_export"}) {
		t.Fatalf("transport calls=%v", transport.calls)
	}
}

func TestSaaSBindingKeepsCapabilityAndValidationParity(t *testing.T) {
	binding, _, direct := openSemanticBinding(t, semanticHost{exporter: pageExportProvider{}}, nil)
	directSummary, err := direct.CapabilitySummary(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	remoteSummary, err := binding.CapabilitySummary(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	directJSON, err := modulecapability.CanonicalJSON(directSummary)
	if err != nil {
		t.Fatal(err)
	}
	remoteJSON, err := modulecapability.CanonicalJSON(remoteSummary)
	if err != nil {
		t.Fatal(err)
	}
	if string(directJSON) != string(remoteJSON) {
		t.Fatalf("capability summaries differ\ndirect=%s\nremote=%s", directJSON, remoteJSON)
	}
	request := modulecapability.ValidationRequest{
		ContractVersion: modulecapability.ValidationContractVersion, ModuleKey: directSummary.Identity.Key,
		CategoryKey: "data_exchange.transfer", ContractSHA256: directSummary.Identity.ContractSHA256,
		Kind: "data_exchange.validation_schema", Candidate: modulecapability.AuthoringFragment{Collection: "model.fields", Key: "import", Value: []byte(`{}`)},
	}
	directResult, directErr := direct.ValidateCapabilityCandidate(t.Context(), request)
	remoteResult, remoteErr := binding.ValidateCapabilityCandidate(t.Context(), request)
	if !reflect.DeepEqual(directResult, remoteResult) || (directErr == nil) != (remoteErr == nil) || (directErr != nil && directErr.Error() != remoteErr.Error()) {
		t.Fatalf("validation parity direct=(%+v,%v) remote=(%+v,%v)", directResult, directErr, remoteResult, remoteErr)
	}
	if directErr == nil || !strings.Contains(directErr.Error(), "validation_scope_invalid") {
		t.Fatalf("validation outside declared scopes was accepted: %v", directErr)
	}
}

var _ modulehost.ExportProvider = pageExportProvider{}
var _ modulehost.JobArtifactOpener = fileExportProvider{}
var _ modulehost.Host = semanticHost{}
