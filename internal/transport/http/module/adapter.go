package module

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	dataexchange "github.com/domainry/domainry-data-exchange-sdk"
	"github.com/domainry/domainry-data-exchange-sdk/modulehost"
	dataexchangeservice "github.com/domainry/domainry-data-exchange/internal/domain/dataexchange/service"
	actioncontract "github.com/domainry/domainry-foundation/action"
	"github.com/domainry/domainry-foundation/apperror"
	"github.com/domainry/domainry-foundation/httpresponse"
	"github.com/domainry/domainry-foundation/modulehttp"
	"github.com/domainry/domainry-foundation/requestcontext"
	identitysdk "github.com/domainry/domainry-identity-sdk"
)

type adapter struct {
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

type jobListBinding interface {
	Jobs(context.Context, dataexchange.JobListRequest) ([]dataexchange.Job, error)
}

type actionScopedJobBinding interface {
	JobForAction(context.Context, dataexchange.JobRequest, string) (dataexchange.Job, error)
}

func (*adapter) ContractVersion() string { return modulehttp.ContractVersion }
func (*adapter) Owner() string           { return dataexchange.DataExchangeHTTPAdapterContract().Owner }
func (*adapter) Name() string            { return dataexchange.DataExchangeHTTPAdapterContract().Name }
func (s *adapter) Handler() http.Handler { return s.mux }
func (s *adapter) Routes() []modulehttp.Route {
	return append([]modulehttp.Route(nil), s.routes...)
}

func NewAdapter(binding jobBinding, host modulehost.Host) (modulehttp.Adapter, error) {
	if binding == nil {
		return nil, errors.New("Data Exchange HTTP binding is unavailable")
	}
	s := &adapter{binding: binding, host: host, mux: http.NewServeMux()}
	var err error
	s.routes, err = dataExchangeRoutes()
	if err != nil {
		return nil, err
	}
	handlers := map[string]http.HandlerFunc{
		dataexchange.ActionDataExchangeJobList:     s.listJobs,
		dataexchange.ActionDataExchangeJobGet:      s.getJob,
		dataexchange.ActionDataExchangeJobCancel:   s.cancelJob,
		dataexchange.ActionDataExchangeJobDownload: s.downloadJob,
	}
	for _, route := range s.routes {
		key := strings.TrimSpace(route.Action.Key)
		handler, found := handlers[key]
		if !found {
			return nil, fmt.Errorf("Data Exchange Action %q has no HTTP handler", key)
		}
		s.mux.HandleFunc(route.Pattern(), handler)
		delete(handlers, key)
	}
	if len(handlers) != 0 {
		keys := make([]string, 0, len(handlers))
		for key := range handlers {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		return nil, fmt.Errorf("Data Exchange HTTP handlers have no Action route entries: %v", keys)
	}
	return s, nil
}

func (s *adapter) listJobs(response http.ResponseWriter, request *http.Request) {
	principal, ok := identitysdk.PrincipalFromContext(request.Context())
	if !ok {
		httpresponse.WriteError(response, request, httpresponse.Failure{Status: http.StatusUnauthorized, Code: "backend.authentication_required"})
		return
	}
	limit := 0
	if raw := strings.TrimSpace(request.URL.Query().Get("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			httpresponse.WriteError(response, request, httpresponse.Failure{Status: http.StatusBadRequest, Code: "backend.data_exchange.invalid_job_list_request", Cause: err})
			return
		}
		limit = parsed
	}
	listRequest := dataexchange.JobListRequest{
		Scope:    dataexchange.Scope{WorkspaceID: strings.TrimSpace(principal.WorkspaceID), ActorID: strings.TrimSpace(principal.UserID), RoleKey: strings.TrimSpace(principal.RoleKey), RequestID: requestcontext.RequestID(request.Context())},
		Provider: strings.TrimSpace(request.URL.Query().Get("provider")), Operation: strings.TrimSpace(request.URL.Query().Get("operation")), Status: strings.TrimSpace(request.URL.Query().Get("status")), Limit: limit,
	}
	if err := listRequest.Validate(); err != nil {
		httpresponse.WriteError(response, request, httpresponse.Failure{Status: http.StatusBadRequest, Code: "backend.data_exchange.invalid_job_list_request", Cause: err})
		return
	}
	binding, supported := s.binding.(jobListBinding)
	if !supported {
		writeError(response, &apperror.AppError{Kind: apperror.KindUnavailable, Code: "backend.data_exchange.job_list_unavailable"})
		return
	}
	jobs, err := binding.Jobs(request.Context(), listRequest)
	if err != nil {
		writeError(response, err)
		return
	}
	items := make([]any, 0, len(jobs))
	for _, job := range jobs {
		projection, projectErr := s.projectJob(request, job, listRequest.Scope)
		if projectErr != nil {
			writeError(response, projectErr)
			return
		}
		items = append(items, projection)
	}
	writeJSON(response, http.StatusOK, map[string]any{"items": items})
}

func dataExchangeRoutes() ([]modulehttp.Route, error) {
	contract := dataexchange.DataExchangeHTTPAdapterContract()
	routes := make([]modulehttp.Route, 0, len(contract.Routes))
	for _, declared := range contract.Routes {
		action, err := exactPermissionAction(declared.Action)
		if err != nil {
			return nil, err
		}
		route, err := modulehttp.RouteFromAction(action)
		if err != nil {
			return nil, fmt.Errorf("project Data Exchange Action %q: %w", declared.Action.Key, err)
		}
		routes = append(routes, route)
	}
	return routes, nil
}

func exactPermissionAction(action actioncontract.ActionDefinition) (actioncontract.ActionDefinition, error) {
	separator := strings.LastIndexByte(strings.TrimSpace(action.Key), '.')
	if separator <= 0 || separator == len(action.Key)-1 {
		return actioncontract.ActionDefinition{}, fmt.Errorf("Data Exchange Action %q cannot define an exact Permission", action.Key)
	}
	if action.Permission == nil {
		operationKey := action.Key[separator+1:]
		action.Permission = &actioncontract.PermissionDefinition{
			Key: action.Key, Owner: action.Owner, ModuleKey: "data_exchange", GroupKey: "data_exchange.jobs",
			ResourceKey: action.Key[:separator], OperationKey: operationKey,
			Label: action.Label, Description: action.OperationLabel, Category: action.CapabilityLabel,
			GrantMode: actioncontract.PermissionGrantInternal, ScopeKind: actioncontract.PermissionScopeNone,
			DefaultLocale: "en", I18n: dataExchangePermissionText(operationKey, action.Label),
			LifecycleStatus: actioncontract.LifecycleActive,
		}
	}
	if action.Permission.Key != action.Key {
		return actioncontract.ActionDefinition{}, fmt.Errorf("Data Exchange Action %q must use its same-key Permission", action.Key)
	}
	return actioncontract.NormalizeDefinition(action)
}

func dataExchangePermissionText(operationKey, englishLabel string) actioncontract.PermissionLocalizedText {
	operationEnglish := map[string]string{"list": "List", "get": "View details", "cancel": "Cancel", "download": "Download result"}[operationKey]
	operationChinese := map[string]string{"list": "查看列表", "get": "查看详情", "cancel": "取消", "download": "下载结果"}[operationKey]
	return actioncontract.PermissionLocalizedText{
		"en":    {ModuleLabel: "Data exchange", GroupLabel: "Exchange jobs", ResourceLabel: "Data exchange jobs", OperationLabel: operationEnglish, Label: englishLabel, Description: "Provides access to Data exchange jobs for the “" + operationEnglish + "” operation."},
		"zh-CN": {ModuleLabel: "数据交换", GroupLabel: "交换任务", ResourceLabel: "数据交换任务", OperationLabel: operationChinese, Label: "数据交换任务 · " + operationChinese, Description: "允许对数据交换任务执行“" + operationChinese + "”操作。"},
	}
}

// CapabilityRoutes returns the exact SDK-derived route manifest used by the
// capability projection without exposing the HTTP handler implementation.
func CapabilityRoutes() ([]modulehttp.Route, error) { return dataExchangeRoutes() }

func (s *adapter) getJob(response http.ResponseWriter, request *http.Request) {
	jobRequest, ok := httpJobRequest(response, request, dataexchange.ActionDataExchangeJobGet)
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

func (s *adapter) cancelJob(response http.ResponseWriter, request *http.Request) {
	jobRequest, ok := httpJobRequest(response, request, dataexchange.ActionDataExchangeJobCancel)
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

func (s *adapter) downloadJob(response http.ResponseWriter, request *http.Request) {
	jobRequest, ok := httpJobRequest(response, request, dataexchange.ActionDataExchangeJobDownload)
	if !ok {
		return
	}
	var job dataexchange.Job
	var err error
	if scoped, supported := s.binding.(actionScopedJobBinding); supported {
		job, err = scoped.JobForAction(request.Context(), jobRequest, dataexchange.ActionDataExchangeJobDownload)
	} else {
		job, err = s.binding.Job(request.Context(), jobRequest)
	}
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

func (s *adapter) openArtifact(request *http.Request, job dataexchange.Job, scope dataexchange.Scope) (dataexchange.Artifact, error) {
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
		Scope: scope, JobID: job.JobID, Provider: job.Provider, Operation: job.Operation,
	})
}

func (s *adapter) projectJob(request *http.Request, job dataexchange.Job, scope dataexchange.Scope) (any, error) {
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

func httpJobRequest(response http.ResponseWriter, request *http.Request, permissionKey string) (dataexchange.JobRequest, bool) {
	principal, ok := identitysdk.PrincipalFromContext(request.Context())
	if !ok {
		httpresponse.WriteError(response, request, httpresponse.Failure{Status: http.StatusUnauthorized, Code: "backend.authentication_required"})
		return dataexchange.JobRequest{}, false
	}
	jobRequest := dataexchange.JobRequest{
		Scope: dataexchange.Scope{
			WorkspaceID: strings.TrimSpace(principal.WorkspaceID), ActorID: strings.TrimSpace(principal.UserID), RoleKey: strings.TrimSpace(principal.RoleKey), RequestID: requestcontext.RequestID(request.Context()),
		},
		JobID: strings.TrimSpace(request.PathValue("jobID")), Provider: strings.TrimSpace(request.URL.Query().Get("provider")), Operation: strings.TrimSpace(request.URL.Query().Get("operation")),
	}
	if err := jobRequest.Validate(); err != nil {
		httpresponse.WriteError(response, request, httpresponse.Failure{Status: http.StatusBadRequest, Code: "backend.data_exchange.invalid_job_request", Cause: err})
		return dataexchange.JobRequest{}, false
	}
	if _, err := dataexchangeservice.ResolveJobAccess(request.Context(), jobRequest, permissionKey); err != nil {
		writeError(response, err)
		return dataexchange.JobRequest{}, false
	}
	return jobRequest, true
}

type jobResponse struct {
	ID          int64     `json:"id"`
	JobID       string    `json:"job_id"`
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
		ID: job.ID, JobID: job.JobID, Provider: job.Provider, Operation: job.Operation, Status: job.Status, ObjectKey: job.ObjectKey,
		Checkpoint: job.Checkpoint, Total: job.Total, ArtifactID: job.ArtifactID, ErrorCode: job.ErrorCode,
		ReferenceID: job.ReferenceID, CreatedAt: job.CreatedAt, UpdatedAt: job.UpdatedAt,
	}
}

func writeError(response http.ResponseWriter, err error) {
	if errors.Is(err, dataexchange.ErrArtifactExpired) {
		httpresponse.WriteError(response, nil, httpresponse.Failure{Status: http.StatusConflict, Code: "backend.data_exchange.artifact_expired", Cause: err})
		return
	}
	if errors.Is(err, dataexchange.ErrJobNotFound) {
		httpresponse.WriteError(response, nil, httpresponse.Failure{Status: http.StatusNotFound, Code: "backend.data_exchange.job_not_found", Cause: err})
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
	httpresponse.WriteError(response, nil, httpresponse.Failure{Status: status, Code: code, Params: apperror.ParamsOf(err), Cause: err})
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Cache-Control", "no-store")
	httpresponse.WriteJSON(response, status, value)
}

var _ modulehttp.Adapter = (*adapter)(nil)
