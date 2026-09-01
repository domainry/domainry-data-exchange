package module

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	dataexchange "github.com/domainry/domainry-data-exchange-sdk"
	"github.com/domainry/domainry-data-exchange-sdk/modulehost"
	actioncontract "github.com/domainry/domainry-foundation/action"
	"github.com/domainry/domainry-foundation/modulehttp"
	identitysdk "github.com/domainry/domainry-identity-sdk"
)

type bindingProbe struct {
	last dataexchange.JobRequest
}

func (*bindingProbe) Descriptor() dataexchange.Descriptor { return dataexchange.Descriptor{} }
func (*bindingProbe) SubmitImport(context.Context, dataexchange.ImportRequest) (dataexchange.Job, bool, error) {
	return dataexchange.Job{}, false, nil
}
func (*bindingProbe) SubmitExport(context.Context, dataexchange.ExportRequest) (dataexchange.Job, bool, error) {
	return dataexchange.Job{}, false, nil
}
func (p *bindingProbe) Job(_ context.Context, request dataexchange.JobRequest) (dataexchange.Job, error) {
	p.last = request
	if request.JobID == "missing" {
		return dataexchange.Job{}, dataexchange.ErrJobNotFound
	}
	return probeJob(request), nil
}
func (p *bindingProbe) Cancel(_ context.Context, request dataexchange.JobRequest) (dataexchange.Job, error) {
	p.last = request
	job := probeJob(request)
	job.Status = "cancelled"
	return job, nil
}
func (*bindingProbe) Download(context.Context, dataexchange.JobRequest) (dataexchange.Artifact, error) {
	return dataexchange.Artifact{Filename: "contacts.csv", ContentType: "text/csv", Size: 10, ExpiresAt: time.Now().UTC().Add(time.Minute), Content: io.NopCloser(strings.NewReader("name\nAcme\n"))}, nil
}
func (*bindingProbe) Start(context.Context, dataexchange.WorkerConfig) <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}
func (*bindingProbe) Close(context.Context) error { return nil }

func probeJob(request dataexchange.JobRequest) dataexchange.Job {
	now := time.Now().UTC()
	status := "running"
	if request.JobID == "completed" {
		status = "completed"
	}
	return dataexchange.Job{
		ID: request.JobID, Provider: "records", Operation: "export", Status: status, ObjectKey: "contact",
		WorkspaceID: request.Scope.WorkspaceID, ActorID: request.Scope.ActorID, Options: []byte("secret"), LeaseOwner: "worker-secret",
		CreatedAt: now, UpdatedAt: now,
	}
}

func authenticatedRequest(method, target string) *http.Request {
	request := httptest.NewRequest(method, target, nil)
	ctx := identitysdk.WithRequestIdentity(request.Context(), identitysdk.RequestIdentity{Principal: identitysdk.Principal{
		Known: true, WorkspaceID: "workspace", UserID: "actor", RoleKey: "member",
	}})
	return request.WithContext(ctx)
}

func TestSurfaceDeclaresAndServesSafeJobManagement(t *testing.T) {
	probe := &bindingProbe{}
	surface, err := NewSurface(probe, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := modulehttp.ValidateSurface(surface); err != nil {
		t.Fatal(err)
	}
	if routes := surface.Routes(); len(routes) != 3 || routes[0].Pattern() != "GET /data-exchange/jobs/{jobID}" || routes[0].Action.Authorization.Strategy != actioncontract.AuthorizationAuthenticatedPrincipal || routes[0].Action.Exposures[0] != modulehttp.ExposurePublic || routes[1].Action.IdempotencyDecision != "natural_key" || routes[2].Pattern() != "GET /data-exchange/jobs/{jobID}/download" {
		t.Fatalf("routes=%+v", routes)
	}
	if operations := surface.(modulehttp.OpenAPIProvider).OpenAPIOperations(); operations["GET /data-exchange/jobs/{jobID}"]["operationId"] != "getDataExchangeJob" || hasOpenAPIParameter(operations["POST /data-exchange/jobs/{jobID}/cancel"], "Idempotency-Key") {
		t.Fatalf("OpenAPI operations=%#v", operations)
	}

	response := httptest.NewRecorder()
	surface.Handler().ServeHTTP(response, authenticatedRequest(http.MethodGet, "/data-exchange/jobs/job-1?provider=records&operation=export"))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if probe.last.Scope.WorkspaceID != "workspace" || probe.last.Scope.ActorID != "actor" || probe.last.Provider != "records" || probe.last.Operation != "export" {
		t.Fatalf("request=%+v", probe.last)
	}
	if body := response.Body.String(); strings.Contains(body, "secret") || strings.Contains(body, "options") || strings.Contains(body, "lease_owner") {
		t.Fatalf("internal job state leaked: %s", body)
	}

	response = httptest.NewRecorder()
	surface.Handler().ServeHTTP(response, authenticatedRequest(http.MethodPost, "/data-exchange/jobs/job-1/cancel"))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"status":"cancelled"`) {
		t.Fatalf("cancel status=%d body=%s", response.Code, response.Body.String())
	}

	response = httptest.NewRecorder()
	surface.Handler().ServeHTTP(response, authenticatedRequest(http.MethodGet, "/data-exchange/jobs/completed/download?provider=records&operation=export"))
	if response.Code != http.StatusOK || response.Body.String() != "name\nAcme\n" || response.Header().Get("Content-Disposition") != "attachment; filename=contacts.csv" {
		t.Fatalf("download status=%d headers=%v body=%q", response.Code, response.Header(), response.Body.String())
	}
	response = httptest.NewRecorder()
	surface.Handler().ServeHTTP(response, authenticatedRequest(http.MethodGet, "/data-exchange/jobs/job-1/download?provider=records&operation=export"))
	if response.Code != http.StatusConflict {
		t.Fatalf("non-terminal download status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestSurfaceFailsClosedForMissingIdentityAndJob(t *testing.T) {
	surface, err := NewSurface(&bindingProbe{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	surface.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/data-exchange/jobs/job-1", nil))
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous status=%d", response.Code)
	}
	response = httptest.NewRecorder()
	surface.Handler().ServeHTTP(response, authenticatedRequest(http.MethodGet, "/data-exchange/jobs/missing"))
	if response.Code != http.StatusNotFound {
		t.Fatalf("missing status=%d body=%s", response.Code, response.Body.String())
	}
}

type projectingHost struct{ provider projectingProvider }

func (*projectingHost) ImportProvider(string) (modulehost.ImportProvider, bool) { return nil, false }
func (h *projectingHost) ExportProvider(string) (modulehost.ExportProvider, bool) {
	return h.provider, true
}

type projectingProvider struct{}

func (projectingProvider) ReadExportPage(context.Context, dataexchange.ExportPageRequest) (dataexchange.ExportPage, error) {
	return dataexchange.ExportPage{}, nil
}
func (projectingProvider) ProjectDataExchangeJob(_ context.Context, job dataexchange.Job, scope dataexchange.Scope) (any, error) {
	return map[string]any{"id": job.ID, "legacy_status": job.Status, "actor": scope.ActorID}, nil
}
func (projectingProvider) OpenDataExchangeArtifact(_ context.Context, _ dataexchange.Job, _ dataexchange.Scope) (dataexchange.Artifact, error) {
	return dataexchange.Artifact{Filename: "provider.csv", ContentType: "text/csv", ExpiresAt: time.Now().UTC().Add(time.Minute), Content: io.NopCloser(strings.NewReader("provider\n"))}, nil
}

func TestSurfaceUsesProviderOwnedJobProjection(t *testing.T) {
	surface, err := NewSurface(&bindingProbe{}, &projectingHost{})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	surface.Handler().ServeHTTP(response, authenticatedRequest(http.MethodGet, "/data-exchange/jobs/job-1?provider=records&operation=export"))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"legacy_status":"running"`) || !strings.Contains(response.Body.String(), `"actor":"actor"`) {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	response = httptest.NewRecorder()
	surface.Handler().ServeHTTP(response, authenticatedRequest(http.MethodGet, "/data-exchange/jobs/completed/download?provider=records&operation=export"))
	if response.Code != http.StatusOK || response.Body.String() != "provider\n" || response.Header().Get("Content-Disposition") != "attachment; filename=provider.csv" {
		t.Fatalf("provider download status=%d headers=%v body=%q", response.Code, response.Header(), response.Body.String())
	}
}

func TestSurfaceFailsClosedWhenExportProviderIsUnavailable(t *testing.T) {
	ownedSurface, err := NewSurface(&bindingProbe{}, &projectingHost{})
	if err != nil {
		t.Fatal(err)
	}
	host := ownedSurface.(*surface)
	host.host = missingProviderHost{}
	response := httptest.NewRecorder()
	host.Handler().ServeHTTP(response, authenticatedRequest(http.MethodGet, "/data-exchange/jobs/completed/download?provider=records&operation=export"))
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), "backend.data_exchange.export_provider_unavailable") {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

type missingProviderHost struct{}

func (missingProviderHost) ImportProvider(string) (modulehost.ImportProvider, bool) {
	return nil, false
}
func (missingProviderHost) ExportProvider(string) (modulehost.ExportProvider, bool) {
	return nil, false
}

func hasOpenAPIParameter(operation map[string]any, name string) bool {
	parameters, _ := operation["parameters"].([]map[string]any)
	for _, parameter := range parameters {
		if parameter["name"] == name {
			return true
		}
	}
	return false
}

var _ jobBinding = (*bindingProbe)(nil)
var _ modulehost.Host = (*projectingHost)(nil)
var _ modulehost.JobProjector = projectingProvider{}
var _ modulehost.JobArtifactOpener = projectingProvider{}
var _ modulehost.Host = missingProviderHost{}
