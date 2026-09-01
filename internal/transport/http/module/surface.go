package module

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	dataexchange "github.com/domainry/domainry-data-exchange-sdk"
	"github.com/domainry/domainry-data-exchange-sdk/modulehost"
	"github.com/domainry/domainry-foundation/apperror"
	"github.com/domainry/domainry-foundation/modulehttp"
	identitysdk "github.com/domainry/domainry-identity-sdk"
)

type surface struct {
	binding jobBinding
	host    modulehost.Host
	mux     *http.ServeMux
	routes  []modulehttp.Route
}

type jobBinding interface {
	Job(context.Context, dataexchange.JobRequest) (dataexchange.Job, error)
	Cancel(context.Context, dataexchange.JobRequest) (dataexchange.Job, error)
	Download(context.Context, dataexchange.JobRequest) (dataexchange.Artifact, error)
}

func (*surface) ContractVersion() string { return modulehttp.ContractVersion }
func (*surface) Owner() string           { return dataexchange.DataExchangeHTTPSurfaceContract().Owner }
func (*surface) Name() string            { return dataexchange.DataExchangeHTTPSurfaceContract().Name }
func (s *surface) Handler() http.Handler { return s.mux }
func (s *surface) Routes() []modulehttp.Route {
	return append([]modulehttp.Route(nil), s.routes...)
}

func (s *surface) OpenAPIOperations() map[string]map[string]any {
	return dataexchange.DataExchangeHTTPSurfaceContract().OpenAPI
}

func NewSurface(binding jobBinding, host modulehost.Host) (modulehttp.Surface, error) {
	if binding == nil {
		return nil, errors.New("Data Exchange HTTP binding is unavailable")
	}
	s := &surface{binding: binding, host: host, mux: http.NewServeMux()}
	s.routes = dataExchangeRoutes()
	s.mux.HandleFunc("GET /data-exchange/jobs/{jobID}", s.getJob)
	s.mux.HandleFunc("POST /data-exchange/jobs/{jobID}/cancel", s.cancelJob)
	s.mux.HandleFunc("GET /data-exchange/jobs/{jobID}/download", s.downloadJob)
	return s, nil
}

func dataExchangeRoutes() []modulehttp.Route {
	contract := dataexchange.DataExchangeHTTPSurfaceContract()
	routes := make([]modulehttp.Route, 0, len(contract.Routes))
	for _, route := range contract.Routes {
		exposures := make([]modulehttp.Exposure, len(route.Exposures))
		for index, exposure := range route.Exposures {
			exposures[index] = modulehttp.Exposure(exposure)
		}
		routes = append(routes, modulehttp.Route{
			Pattern: route.Pattern, Exposures: exposures, Authentication: modulehttp.Authentication(route.Authentication), Permission: route.Permission,
			AnyPermissions: append([]string(nil), route.AnyPermissions...), PrincipalOnly: route.PrincipalOnly,
			Governance: &modulehttp.Governance{EffectClass: modulehttp.EffectClass(route.EffectClass), HighRiskPolicy: modulehttp.HighRiskPolicy(route.HighRiskPolicy), IdempotencyDecision: route.IdempotencyDecision, AuditClass: route.AuditClass},
		})
	}
	return routes
}

// CapabilityRoutes returns the exact SDK-derived route manifest used by the
// capability projection without exposing the HTTP handler implementation.
func CapabilityRoutes() []modulehttp.Route { return dataExchangeRoutes() }

func (s *surface) getJob(response http.ResponseWriter, request *http.Request) {
	jobRequest, ok := httpJobRequest(response, request)
	if !ok {
		return
	}
	job, err := s.binding.Job(request.Context(), jobRequest)
	if err != nil {
		writeError(response, err)
		return
	}
	projection, err := s.projectJob(request, job, jobRequest.Scope)
	if err != nil {
		writeError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, projection)
}

func (s *surface) cancelJob(response http.ResponseWriter, request *http.Request) {
	jobRequest, ok := httpJobRequest(response, request)
	if !ok {
		return
	}
	job, err := s.binding.Cancel(request.Context(), jobRequest)
	if err != nil {
		writeError(response, err)
		return
	}
	projection, err := s.projectJob(request, job, jobRequest.Scope)
	if err != nil {
		writeError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, projection)
}

func (s *surface) downloadJob(response http.ResponseWriter, request *http.Request) {
	jobRequest, ok := httpJobRequest(response, request)
	if !ok {
		return
	}
	job, err := s.binding.Job(request.Context(), jobRequest)
	if err != nil {
		writeError(response, err)
		return
	}
	if job.Operation != "export" || job.Status != "completed" {
		writeError(response, &apperror.AppError{Kind: apperror.KindConflict, Code: "backend.data_exchange.result_not_ready"})
		return
	}
	artifact, err := s.openArtifact(request, job, jobRequest.Scope)
	if err != nil {
		writeError(response, err)
		return
	}
	if artifact.Content == nil {
		writeError(response, errors.New("Data Exchange artifact content is unavailable"))
		return
	}
	defer artifact.Content.Close()
	if !artifact.ExpiresAt.IsZero() && !time.Now().UTC().Before(artifact.ExpiresAt) {
		writeError(response, dataexchange.ErrArtifactExpired)
		return
	}
	contentType := strings.TrimSpace(artifact.ContentType)
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	filename := strings.TrimSpace(artifact.Filename)
	if filename == "" {
		filename = "data-exchange-export"
	}
	response.Header().Set("Content-Type", contentType)
	response.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": filename}))
	response.Header().Set("Cache-Control", "no-store, private")
	if artifact.Size > 0 {
		response.Header().Set("Content-Length", strconv.FormatInt(artifact.Size, 10))
	}
	response.WriteHeader(http.StatusOK)
	_, _ = io.Copy(response, artifact.Content)
}

func (s *surface) openArtifact(request *http.Request, job dataexchange.Job, scope dataexchange.Scope) (dataexchange.Artifact, error) {
	if s.host != nil && job.Operation == "export" {
		provider, found := s.host.ExportProvider(job.Provider)
		if !found {
			return dataexchange.Artifact{}, &apperror.AppError{Kind: apperror.KindUnavailable, Code: "backend.data_exchange.export_provider_unavailable"}
		}
		if opener, ok := provider.(modulehost.JobArtifactOpener); ok {
			return opener.OpenDataExchangeArtifact(request.Context(), job, scope)
		}
	}
	return s.binding.Download(request.Context(), dataexchange.JobRequest{
		Scope: scope, JobID: job.ID, Provider: job.Provider, Operation: job.Operation,
	})
}

func (s *surface) projectJob(request *http.Request, job dataexchange.Job, scope dataexchange.Scope) (any, error) {
	if s.host != nil {
		var provider any
		var found bool
		switch job.Operation {
		case "import":
			provider, found = s.host.ImportProvider(job.Provider)
		case "export":
			provider, found = s.host.ExportProvider(job.Provider)
		}
		if projector, ok := provider.(modulehost.JobProjector); found && ok {
			return projector.ProjectDataExchangeJob(request.Context(), job, scope)
		}
	}
	return projectJob(job), nil
}

func httpJobRequest(response http.ResponseWriter, request *http.Request) (dataexchange.JobRequest, bool) {
	principal, ok := identitysdk.PrincipalFromContext(request.Context())
	if !ok {
		writeJSON(response, http.StatusUnauthorized, map[string]string{"code": "backend.authentication_required"})
		return dataexchange.JobRequest{}, false
	}
	jobRequest := dataexchange.JobRequest{
		Scope: dataexchange.Scope{
			WorkspaceID: strings.TrimSpace(principal.WorkspaceID), ActorID: strings.TrimSpace(principal.UserID), RoleKey: strings.TrimSpace(principal.RoleKey), RequestID: strings.TrimSpace(request.Header.Get("X-Request-ID")),
		},
		JobID: strings.TrimSpace(request.PathValue("jobID")), Provider: strings.TrimSpace(request.URL.Query().Get("provider")), Operation: strings.TrimSpace(request.URL.Query().Get("operation")),
	}
	if err := jobRequest.Validate(); err != nil {
		writeJSON(response, http.StatusBadRequest, map[string]string{"code": "backend.data_exchange.invalid_job_request"})
		return dataexchange.JobRequest{}, false
	}
	return jobRequest, true
}

type jobResponse struct {
	ID          string    `json:"id"`
	Provider    string    `json:"provider"`
	Operation   string    `json:"operation"`
	Status      string    `json:"status"`
	ObjectKey   string    `json:"object_key"`
	Checkpoint  int       `json:"checkpoint"`
	Total       int       `json:"total"`
	ArtifactID  string    `json:"artifact_id,omitempty"`
	ErrorCode   string    `json:"error_code,omitempty"`
	ReferenceID string    `json:"reference_id,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func projectJob(job dataexchange.Job) jobResponse {
	return jobResponse{
		ID: job.ID, Provider: job.Provider, Operation: job.Operation, Status: job.Status, ObjectKey: job.ObjectKey,
		Checkpoint: job.Checkpoint, Total: job.Total, ArtifactID: job.ArtifactID, ErrorCode: job.ErrorCode,
		ReferenceID: job.ReferenceID, CreatedAt: job.CreatedAt, UpdatedAt: job.UpdatedAt,
	}
}

func writeError(response http.ResponseWriter, err error) {
	if errors.Is(err, dataexchange.ErrArtifactExpired) {
		writeJSON(response, http.StatusConflict, map[string]string{"code": "backend.data_exchange.artifact_expired"})
		return
	}
	if errors.Is(err, dataexchange.ErrJobNotFound) {
		writeJSON(response, http.StatusNotFound, map[string]string{"code": "backend.data_exchange.job_not_found"})
		return
	}
	status := http.StatusInternalServerError
	switch apperror.KindOf(err) {
	case apperror.KindBadRequest:
		status = http.StatusBadRequest
	case apperror.KindForbidden:
		status = http.StatusForbidden
	case apperror.KindNotFound:
		status = http.StatusNotFound
	case apperror.KindConflict:
		status = http.StatusConflict
	case apperror.KindRateLimited:
		status = http.StatusTooManyRequests
	case apperror.KindUnavailable:
		status = http.StatusServiceUnavailable
	}
	code := apperror.CodeOf(err)
	if code == "" || status == http.StatusInternalServerError {
		code = "backend.data_exchange.internal"
	}
	writeJSON(response, status, map[string]string{"code": code})
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json")
	response.Header().Set("Cache-Control", "no-store")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}

var _ modulehttp.Surface = (*surface)(nil)
var _ modulehttp.OpenAPIProvider = (*surface)(nil)
