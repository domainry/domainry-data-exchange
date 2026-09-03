package service

import (
	"context"
	"strings"

	dataexchange "github.com/domainry/domainry-data-exchange-sdk"
	"github.com/domainry/domainry-data-exchange/internal/domain/dataexchange/model"
	"github.com/domainry/domainry-foundation/apperror"
	identitysdk "github.com/domainry/domainry-identity-sdk"
)

// ResolveJobAccess verifies the exact function and data grants for one job
// operation. Data Exchange job management accepts only canonical `owner`,
// compiled as owner_user_id == $subject.id and pushed into storage as actor_id.
func ResolveJobAccess(ctx context.Context, request dataexchange.JobRequest, permissionKey string) (model.JobAccess, error) {
	principal, ok := identitysdk.PrincipalFromContext(ctx)
	if !ok || principal.AccessBundle == nil {
		return model.JobAccess{}, jobPermissionDenied(permissionKey)
	}
	workspace, actor := strings.TrimSpace(request.Scope.WorkspaceID), strings.TrimSpace(request.Scope.ActorID)
	subjectWorkspace := strings.TrimSpace(string(principal.AccessBundle.Subject.WorkspaceID))
	subjectID := strings.TrimSpace(string(principal.AccessBundle.Subject.SubjectID))
	if workspace == "" || actor == "" || workspace != strings.TrimSpace(principal.WorkspaceID) || actor != strings.TrimSpace(principal.UserID) || workspace != subjectWorkspace || actor != subjectID {
		return model.JobAccess{}, jobPermissionDenied(permissionKey)
	}
	permissionKey = strings.TrimSpace(permissionKey)
	resource, action, ok := permissionParts(permissionKey)
	if !ok || !hasExactFunctionGrant(*principal.AccessBundle, resource, action) {
		return model.JobAccess{}, jobPermissionDenied(permissionKey)
	}

	matchedAllow := false
	for _, policy := range principal.AccessBundle.DataPolicies {
		if strings.TrimSpace(policy.Key) != permissionKey || strings.TrimSpace(string(policy.Resource)) != resource || strings.TrimSpace(string(policy.Action)) != action {
			continue
		}
		if policy.Effect != identitysdk.EffectAllow || len(policy.DataScopes) != 1 || policy.DataScopes[0] != identitysdk.DataScopeOwner || !isOwnerPredicate(policy.Predicate) {
			return model.JobAccess{}, jobPermissionDenied(permissionKey)
		}
		matchedAllow = true
	}
	if !matchedAllow {
		return model.JobAccess{}, jobPermissionDenied(permissionKey)
	}
	for _, guardrail := range principal.AccessBundle.Guardrails {
		resourceMatches := guardrail.Resource == "" || guardrail.Resource == "*" || strings.TrimSpace(string(guardrail.Resource)) == resource
		actionMatches := guardrail.Action == "" || guardrail.Action == "*" || strings.TrimSpace(string(guardrail.Action)) == action
		if !resourceMatches || !actionMatches || guardrail.Effect != identitysdk.EffectDeny || strings.TrimSpace(guardrail.Field) != "" {
			continue
		}
		return model.JobAccess{}, jobPermissionDenied(permissionKey)
	}
	return model.JobAccess{PermissionKey: permissionKey, WorkspaceID: workspace, OwnerActorID: subjectID}.Normalized(), nil
}

func isOwnerPredicate(predicate identitysdk.Predicate) bool {
	value, ok := predicate.Value.(string)
	return ok && strings.TrimSpace(predicate.Fact) == "owner_user_id" && predicate.Operator == identitysdk.OperatorEqual && strings.TrimSpace(value) == "$subject.id" &&
		len(predicate.Path) == 0 && len(predicate.All) == 0 && len(predicate.Any) == 0 && predicate.Not == nil
}

func permissionParts(permissionKey string) (string, string, bool) {
	permissionKey = strings.TrimSpace(permissionKey)
	separator := strings.LastIndexByte(permissionKey, '.')
	if separator <= 0 || separator == len(permissionKey)-1 {
		return "", "", false
	}
	return permissionKey[:separator], permissionKey[separator+1:], true
}

func hasExactFunctionGrant(bundle identitysdk.AccessBundle, resource, action string) bool {
	allowed := false
	for _, grant := range bundle.FunctionGrants {
		if strings.TrimSpace(string(grant.Resource)) != resource || strings.TrimSpace(string(grant.Action)) != action {
			continue
		}
		if grant.Effect == identitysdk.EffectDeny {
			return false
		}
		allowed = allowed || grant.Effect == identitysdk.EffectAllow
	}
	return allowed
}

func jobPermissionDenied(permissionKey string) error {
	return &apperror.AppError{Kind: apperror.KindForbidden, Code: "backend.data_exchange.job_permission_denied", Params: map[string]string{"permission_key": strings.TrimSpace(permissionKey)}}
}
