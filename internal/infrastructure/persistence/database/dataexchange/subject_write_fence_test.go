package dataexchange

import (
	"database/sql"
	"testing"

	ormdialect "github.com/domainry/domainry-orm/dialect"
	_ "modernc.org/sqlite"
)

func TestSubjectErasureFenceJoinsBusinessRequestID(t *testing.T) {
	database, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	for _, statement := range []string{
		"CREATE TABLE _subject_requests (id INTEGER PRIMARY KEY AUTOINCREMENT, request_id TEXT NOT NULL, workspace_id TEXT NOT NULL, request_type TEXT NOT NULL, kind TEXT NOT NULL, resolved_identity TEXT NOT NULL)",
		"CREATE TABLE _subject_steps (id INTEGER PRIMARY KEY AUTOINCREMENT, workspace_id TEXT NOT NULL, request_id TEXT NOT NULL, owner TEXT NOT NULL, operation TEXT NOT NULL)",
		"INSERT INTO _subject_requests (request_id,workspace_id,request_type,kind,resolved_identity) VALUES ('request-business','workspace','internal','erase','subject')",
		"INSERT INTO _subject_steps (workspace_id,request_id,owner,operation) VALUES ('workspace','request-business','lifecycle','erase_fence')",
	} {
		if _, err := database.ExecContext(t.Context(), statement); err != nil {
			t.Fatal(err)
		}
	}
	renderer, err := ormdialect.ParseRenderer("sqlite", "", "")
	if err != nil {
		t.Fatal(err)
	}
	for _, scope := range []struct {
		workspace, subject, request string
		found                       bool
	}{
		{"workspace", "subject", "request-business", true},
		{"workspace", "subject", "", true},
		{"other-workspace", "subject", "", false},
		{"workspace", "other-subject", "", false},
		{"workspace", "subject", "another-request", false},
	} {
		statement, args, err := sharedSubjectFenceRequests(renderer, scope.workspace, scope.subject, scope.request).Build()
		if err != nil {
			t.Fatal(err)
		}
		var id string
		err = database.QueryRowContext(t.Context(), statement, args...).Scan(&id)
		if scope.found {
			if err != nil || id != "request-business" {
				t.Fatalf("scope=%+v id=%q err=%v", scope, id, err)
			}
		} else if err != sql.ErrNoRows {
			t.Fatalf("scope=%+v unexpectedly matched id=%q err=%v", scope, id, err)
		}
	}
}
