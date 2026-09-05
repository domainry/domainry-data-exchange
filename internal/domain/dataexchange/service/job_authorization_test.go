package service

import (
	"context"
	"testing"

	dataexchange "github.com/domainry/domainry-data-exchange-sdk"
	"github.com/domainry/domainry-foundation/apperror"
	identitysdk "github.com/domainry/domainry-identity-sdk"
)

func TestResolveJobAccessUsesOnlySameExactPermissionOwnerScope(t *testing.T) {
	request := dataexchange.JobRequest{Scope: dataexchange.Scope{WorkspaceID: "workspace", ActorID: "manager"}, JobID: "job-1"}
	ctx := authorizationContext(authorizationSubject(), dataexchange.ActionDataExchangeJobGet, identitysdk.DataScopeOwner, true, true)
	access, err := ResolveJobAccess(ctx, request, dataexchange.ActionDataExchangeJobGet)
	if err != nil {
		t.Fatal(err)
	}
	if access.PermissionKey != dataexchange.ActionDataExchangeJobGet || access.WorkspaceID != "workspace" || access.OwnerActorID != "manager" {
		t.Fatalf("access=%+v", access)
	}

	for _, test := range []struct {
		name                      string
		permission                string
		functionGrant, dataPolicy bool
	}{
		{name: "function only", permission: dataexchange.ActionDataExchangeJobGet, functionGrant: true},
		{name: "data only", permission: dataexchange.ActionDataExchangeJobGet, dataPolicy: true},
		{name: "different exact key", permission: dataexchange.ActionDataExchangeJobDownload, functionGrant: true, dataPolicy: true},
		{name: "broader all scope", permission: dataexchange.ActionDataExchangeJobGet, functionGrant: true, dataPolicy: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			scope := identitysdk.DataScopeOwner
			if test.name == "broader all scope" {
				scope = identitysdk.DataScopeAll
			}
			ctx := authorizationContext(authorizationSubject(), test.permission, scope, test.functionGrant, test.dataPolicy)
			if _, err := ResolveJobAccess(ctx, request, dataexchange.ActionDataExchangeJobGet); apperror.KindOf(err) != apperror.KindForbidden {
				t.Fatalf("error=%v", err)
			}
		})
	}

	for _, test := range []struct {
		name   string
		mutate func(*identitysdk.AccessBundle)
	}{
		{name: "different policy resource", mutate: func(bundle *identitysdk.AccessBundle) {
			bundle.DataPolicies[0].Resource = "data_exchange.other"
		}},
		{name: "owner with wrong predicate", mutate: func(bundle *identitysdk.AccessBundle) {
			bundle.DataPolicies[0].Predicate = identitysdk.Predicate{Fact: "owner_org_id", Operator: identitysdk.OperatorEqual, Value: "$subject.org_id"}
		}},
		{name: "matching guardrail", mutate: func(bundle *identitysdk.AccessBundle) {
			bundle.Guardrails = []identitysdk.Guardrail{{Resource: "data_exchange.jobs", Action: "get", Effect: identitysdk.EffectDeny}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := authorizationContext(authorizationSubject(), dataexchange.ActionDataExchangeJobGet, identitysdk.DataScopeOwner, true, true)
			identity, _ := identitysdk.RequestIdentityFromContext(ctx)
			test.mutate(identity.Principal.AccessBundle)
			ctx = identitysdk.WithRequestIdentity(ctx, identity)
			if _, err := ResolveJobAccess(ctx, request, dataexchange.ActionDataExchangeJobGet); apperror.KindOf(err) != apperror.KindForbidden {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestResolveJobAccessRejectsMixedPrincipalIdentityTypes(t *testing.T) {
	request := dataexchange.JobRequest{Scope: dataexchange.Scope{WorkspaceID: "workspace", ActorID: "user-1"}, JobID: "job-1"}
	for _, test := range []struct {
		name   string
		mutate func(*identitysdk.Principal)
	}{
		{name: "principal user differs from requester", mutate: func(principal *identitysdk.Principal) {
			principal.UserID = "member-1"
		}},
		{name: "bundle subject differs from requester", mutate: func(principal *identitysdk.Principal) {
			principal.AccessBundle.Subject.SubjectID = "account-1"
		}},
		{name: "bundle workspace differs from principal", mutate: func(principal *identitysdk.Principal) {
			principal.AccessBundle.Subject.WorkspaceID = "workspace-other"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := authorizationContext(identitysdk.Subject{WorkspaceID: "workspace", SubjectID: "user-1"}, dataexchange.ActionDataExchangeJobGet, identitysdk.DataScopeOwner, true, true)
			identity, _ := identitysdk.RequestIdentityFromContext(ctx)
			test.mutate(&identity.Principal)
			ctx = identitysdk.WithRequestIdentity(ctx, identity)
			if _, err := ResolveJobAccess(ctx, request, dataexchange.ActionDataExchangeJobGet); apperror.KindOf(err) != apperror.KindForbidden {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func authorizationSubject() identitysdk.Subject {
	return identitysdk.Subject{WorkspaceID: "workspace", SubjectID: "manager"}
}

func authorizationContext(subject identitysdk.Subject, permission string, scope identitysdk.DataScope, functionGrant, dataPolicy bool) context.Context {
	resource, action, _ := permissionParts(permission)
	bundle := &identitysdk.AccessBundle{ContractVersion: identitysdk.CurrentPolicyBundleVersion, Subject: subject}
	if functionGrant {
		bundle.FunctionGrants = []identitysdk.FunctionGrant{{Resource: identitysdk.ResourceType(resource), Action: identitysdk.Action(action), Effect: identitysdk.EffectAllow}}
	}
	if dataPolicy {
		predicate := identitysdk.Predicate{}
		if scope == identitysdk.DataScopeOwner {
			predicate = identitysdk.Predicate{Fact: "owner_user_id", Operator: identitysdk.OperatorEqual, Value: "$subject.id"}
		}
		bundle.DataPolicies = []identitysdk.DataPolicy{{Key: "data-" + permission + "-0", Resource: identitysdk.ResourceType(resource), Action: identitysdk.Action(action), Effect: identitysdk.EffectAllow, DataScopes: []identitysdk.DataScope{scope}, Predicate: predicate}}
	}
	return identitysdk.WithRequestIdentity(context.Background(), identitysdk.RequestIdentity{Principal: identitysdk.Principal{
		Known: true, WorkspaceID: string(subject.WorkspaceID), UserID: string(subject.SubjectID), AccessBundle: bundle,
	}})
}
