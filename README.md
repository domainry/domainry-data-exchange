# Domainry Data Exchange

Data Exchange owns the engineering mechanics of large tabular import/export. Runtime applications provide authorized domain operations through the SDK Provider interfaces; they do not own upload chunks, worker checkpoints, generated artifacts, or file parsing loops.

The `fileengine` package is the single bounded CSV/chunk implementation used by
the durable Module and by Runtime's temporary synchronous compatibility paths.
Keeping it in this repository prevents Runtime from growing a second parser or
artifact generator with different limits and failure semantics.

## Deployments

- `module.Factory` runs in-process and owns `data_exchange_jobs`, `data_exchange_chunks`, `data_exchange_artifacts`, and the workspace queue-scope index in the host database.
- `remote.Factory` delegates the same SDK Binding to a SaaS transport. The transport must connect the Runtime Provider bridge before accepting work.

Both deployments expose the same `dataexchange.Binding`. Runtime composition selects a Factory; HTTP and Record application code do not branch on deployment mode.

## Durable invariants

- Uploads are read incrementally into bounded chunks; the complete file is never required in one memory buffer.
- Import validates the complete source before any Apply batch runs.
- Idempotency compares the immutable source/request fingerprint.
- Export result chunk, next cursor, and progress commit atomically.
- Each generated export page is format-validated and capped at 16 MiB before
  durable commit, preventing a provider page from becoming an unbounded buffer.
- Expired leases resume from the last committed cursor.
- Heartbeats and fencing tokens prevent a reclaimed worker from committing stale work.
- Workspace queue discovery is separated from workspace-scoped job access for PostgreSQL RLS compatibility.
- Artifact downloads stream ordered result chunks.

White-label presentation is intentionally outside this module.
