package dataexchange

import (
	"reflect"
	"strings"
	"testing"

	dataexchangesdk "github.com/domainry/domainry-data-exchange-sdk"
	dataexchangemodel "github.com/domainry/domainry-data-exchange/internal/domain/dataexchange/model"
	persistenceengine "github.com/domainry/domainry-data-exchange/internal/infrastructure/persistence"
	"github.com/domainry/domainry-orm/query"
)

func TestJobOwnerAccessSQLPushesWorkspaceAndActor(t *testing.T) {
	engine, err := persistenceengine.NewEngine("postgres")
	if err != nil {
		t.Fatal(err)
	}
	renderer := engine.Dialect().WithSchema("")
	request := dataexchangesdk.JobRequest{Scope: dataexchangesdk.Scope{WorkspaceID: "workspace", ActorID: "manager"}, JobID: "job-1", Provider: "records", Operation: "export"}

	access := dataexchangemodel.JobAccess{WorkspaceID: "workspace", OwnerActorID: "manager"}
	statement, args, buildErr := query.NewWorkspaceSelectBuilder(renderer, "_data_exchange_jobs", access.WorkspaceID).
		Columns("id").Where(jobCandidatePredicate(request, access, "")).Build()
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	for _, fragment := range []string{`"workspace_id" = $1`, `"id" = $2`, `"actor_id" = $3`, `"provider" = $4`, `"operation" = $5`} {
		if !strings.Contains(statement, fragment) {
			t.Fatalf("SQL %q lacks %q", statement, fragment)
		}
	}
	for _, fragment := range []string{`"requester_org_id"`, "1 = 1"} {
		if strings.Contains(statement, fragment) {
			t.Fatalf("SQL %q unexpectedly contains %q", statement, fragment)
		}
	}
	if !reflect.DeepEqual(args, []any{"workspace", "job-1", "manager", "records", "export"}) {
		t.Fatalf("args=%#v SQL=%s", args, statement)
	}
}

func TestCancelPrecheckAndDMLRepeatWorkspaceAndActor(t *testing.T) {
	engine, err := persistenceengine.NewEngine("postgres")
	if err != nil {
		t.Fatal(err)
	}
	store := &Store{engine: engine, renderer: engine.Dialect().WithSchema("")}
	request := dataexchangesdk.JobRequest{Scope: dataexchangesdk.Scope{WorkspaceID: "workspace", ActorID: "manager"}, JobID: "job-1"}
	access := dataexchangemodel.JobAccess{WorkspaceID: "workspace", OwnerActorID: "manager"}

	precheck, precheckArgs, err := store.jobSelect(request, access, true).Build()
	if err != nil {
		t.Fatal(err)
	}
	update, updateArgs, err := query.NewWorkspaceUpdateBuilder(store.renderer, "_data_exchange_jobs", access.WorkspaceID).
		Set("status", "cancelled").Where(query.And(jobCandidatePredicate(request, access, ""), query.In("status", "queued", "running"))).Build()
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{precheck, update} {
		if !strings.Contains(statement, `"workspace_id"`) || !strings.Contains(statement, `"id"`) || !strings.Contains(statement, `"actor_id"`) {
			t.Fatalf("scoped cancel SQL=%s", statement)
		}
		where := statement[strings.Index(statement, " WHERE ")+len(" WHERE "):]
		if strings.Contains(where, `"requester_org_id"`) {
			t.Fatalf("owner scope added an organization predicate: %s", statement)
		}
	}
	if !strings.HasSuffix(precheck, "FOR UPDATE") || !reflect.DeepEqual(precheckArgs, []any{"workspace", "job-1", "manager"}) || !reflect.DeepEqual(updateArgs, []any{"cancelled", "workspace", "job-1", "manager", "queued", "running"}) {
		t.Fatalf("precheck=%s %#v update=%s %#v", precheck, precheckArgs, update, updateArgs)
	}
}
