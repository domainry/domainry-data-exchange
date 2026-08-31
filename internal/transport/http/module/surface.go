package module

import (
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
	binding dataexchange.Binding
	host    modulehost.Host
	mux     *http.ServeMux
	routes  []modulehttp.Route
}

func (*surface) ContractVersion() string { return modulehttp.ContractVersion }
func (*surface) Owner() string           { return "data_exchange" }
func (*surface) Name() string            { return "job_management" }
func (s *surface) Handler() http.Handler { return s.mux }
func (s *surface) Routes() []modulehttp.Route {
	return append([]modulehttp.Route(nil), s.routes...)
}

func (s *surface) OpenAPIOperations() map[string]map[string]any {
	return map[string]map[string]any{
		"GET /data-exchange/jobs/{jobID}":          jobOpenAPIOperation("getDataExchangeJob", "Get an actor-owned Data Exchange job"),
		"POST /data-exchange/jobs/{jobID}/cancel":  jobOpenAPIOperation("cancelDataExchangeJob", "Cancel an actor-owned Data Exchange job"),
		"GET /data-exchange/jobs/{jobID}/download": jobDownloadOpenAPIOperation(),
	}
}

func NewSurface(binding dataexchange.Binding, host modulehost.Host) (modulehttp.Surface, error) {
	if binding == nil {
		return nil, errors.New("Data Exchange HTTP binding is unavailable")
	}
	s := &surface{binding: binding, host: host, mux: http.NewServeMux()}
	s.routes = []modulehttp.Route{
		jobRoute("GET /data-exchange/jobs/{jobID}", modulehttp.EffectRead, "not_applicable", "owner_read_audit_policy"),
		jobRoute("POST /data-exchange/jobs/{jobID}/cancel", modulehttp.EffectWrite, "natural_key", "mutation_audit_required"),
		jobRoute("GET /data-exchange/jobs/{jobID}/download", modulehttp.EffectRead, "not_applicable", "business_export_download_audit"),
	}
	s.mux.HandleFunc("GET /data-exchange/jobs/{jobID}", s.getJob)
	s.mux.HandleFunc("POST /data-exchange/jobs/{jobID}/cancel", s.cancelJob)
	s.mux.HandleFunc("GET /data-exchange/jobs/{jobID}/download", s.downloadJob)
	return s, nil
}

func jobRoute(pattern string, effect modulehttp.EffectClass, idempotency, audit string) modulehttp.Route {
	return modulehttp.Route{
		Pattern: pattern, Exposures: []modulehttp.Exposure{modulehttp.ExposurePublic},
		Authentication: modulehttp.AuthenticationAuthenticated, PrincipalOnly: true,
		Governance: &modulehttp.Governance{
			EffectClass: effect, HighRiskPolicy: modulehttp.HighRiskNone,
			IdempotencyDecision: idempotency, AuditClass: audit,
		},
	}
}

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

func jobDownloadOpenAPIOperation() map[string]any {
	operation := jobOpenAPIOperation("downloadDataExchangeJob", "Download an actor-owned completed Data Exchange export")
	operation["responses"] = map[string]any{
		"200": map[string]any{"description": "Export artifact", "content": map[string]any{"application/octet-stream": map[string]any{"schema": map[string]any{"type": "string", "format": "binary"}}}},
		"404": map[string]any{"description": "Job not found"}, "409": map[string]any{"description": "Artifact unavailable"},
	}
	return operation
}

func jobOpenAPIOperation(operationID, summary string) map[string]any {
	parameters := []map[string]any{
		{"name": "jobID", "in": "path", "required": true, "schema": map[string]any{"type": "string"}},
		{"name": "provider", "in": "query", "required": false, "schema": map[string]any{"type": "string"}},
		{"name": "operation", "in": "query", "required": false, "schema": map[string]any{"type": "string", "enum": []string{"import", "export"}}},
	}
	return map[string]any{
		"operationId": operationID, "summary": summary, "tags": []string{"Data Exchange"},
		"security": []any{map[string]any{"BearerAuth": []any{}}}, "parameters": parameters,
		"responses": map[string]any{"200": map[string]any{
			"description": "Provider-owned Data Exchange job projection",
			"content":     map[string]any{"application/json": map[string]any{"schema": map[string]any{"type": "object", "additionalProperties": true}}},
		}},
		"x-domainry-runtime-client-method": operationID,
	}
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json")
	response.Header().Set("Cache-Control", "no-store")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}

var _ modulehttp.Surface = (*surface)(nil)
var _ modulehttp.OpenAPIProvider = (*surface)(nil)
