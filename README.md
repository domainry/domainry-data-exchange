# Domainry Data Exchange

Agent-facing question index and source-owned guides: [`capability/agent/index.json`](capability/agent/index.json).

Data Exchange owns the engineering mechanics of large tabular import/export. Runtime applications provide authorized domain operations through the SDK Provider interfaces; they do not own upload chunks, worker checkpoints, generated artifacts, or file parsing loops.

The deployment-neutral SDK owns the single bounded CSV codec. The durable
Module owns the only import orchestration and chunk persistence path; Runtime
does not import implementation packages or maintain a second file engine.

## Deployments

- `module.Factory` runs in-process and owns `_data_exchange_jobs` and `_data_exchange_job_chunks`; output metadata and job bindings use the host's shared Artifact store, and workspace discovery uses shared Worker Scopes.
- `remote.Factory` delegates the same SDK Binding to a SaaS transport. The transport must connect the Runtime Provider bridge before accepting work.

Both deployments expose the same `dataexchange.Binding`. Runtime composition selects a Factory; HTTP and Record application code do not branch on deployment mode.

Both bindings also expose the Data Exchange-owned `modulehttp.Adapter` for
`GET /data-exchange/jobs/{jobID}` and
`POST /data-exchange/jobs/{jobID}/cancel`, plus
`GET /data-exchange/jobs/{jobID}/download`. The host authenticates the request;
each route requires its same-key `data_exchange.jobs.*` Permission and that
exact grant's canonical `owner` data scope before repository access. Data
Exchange derives workspace/actor identity from that authenticated principal
and never serializes raw provider options, lease state, or fencing tokens. An
optional provider-owned projector can preserve an existing public Record or
Report job response without moving domain payload decoding into Data Exchange.

The durable job is the authorization root. Artifact rows and chunks remain
linked through the authorized job and do not carry a second copy of role or
data-scope policy. Owner scope is pushed into SQL as the authenticated
`actor_id`, and workspace isolation is always part of the same query. Cancel
performs its scoped candidate read and final scoped update in one transaction.
There is currently no bulk job mutation HTTP contract, so no synthetic batch
API is provided.

Import and export providers remain responsible for authorizing their business
rows with the source module's own exact Permission and data scope. A Data
Exchange job grant controls only durable job/artifact lifecycle and cannot widen
the provider's business-data access.

## Durable invariants

- Uploads are read incrementally into bounded chunks; the complete file is never required in one memory buffer.
- Import validates the complete source before any Apply batch runs.
- Idempotency compares the immutable source/request fingerprint.
- Task identity and idempotent replay include the workspace and actor, so two
  users may use the same key without sharing a task.
- Export result chunk, next cursor, and progress commit atomically.
- Each generated export page is format-validated and capped at 16 MiB before
  durable commit, preventing a provider page from becoming an unbounded buffer.
- Expired leases resume from the last committed cursor.
- Heartbeats and fencing tokens prevent a reclaimed worker from committing stale work.
- Processing failures retry three times with bounded exponential backoff before
  the job enters its terminal failed/dead-letter state.
- Workspace queue discovery is separated from workspace-scoped job access for PostgreSQL RLS compatibility.
- Artifact downloads stream ordered result chunks.

White-label presentation is intentionally outside this module.

## Account erasure

`SubjectLifecycleBinding` supplies a privileged owner port to Lifecycle; it is
not mounted on the job management HTTP routes. Preparation locks the subject,
rejects running work, cancels queued work, and persists the exact job inventory.
The same subject lock prevents a competing upload/export from committing after
the erasure fence. Legal holds prevent preparation and execution.

Execution removes source/result chunks and artifacts and clears personal
request content, filenames, cursors, references, and raw idempotency keys from
jobs in one source-owned transaction. Stable task IDs and a redacted terminal
state remain. A persisted outcome makes retries deterministic; stale workers
cannot recreate chunks or artifacts. Other actors and workspaces are excluded.
A SaaS owner must implement `SubjectLifecycleTransport` to participate.

## Repository architecture

The implementation follows the same inward dependency direction as the Party
module while preserving the existing Data Exchange deployment contract:

- `internal/domain/dataexchange` owns durable worker state, request policies,
  and repository ports.
- `internal/application/dataexchange` owns import/export orchestration and the
  durable worker lifecycle.
- `internal/adapter/dataexchangesdk` is the SDK-facing binding adapter.
- `internal/assembly/module` and `internal/assembly/saas` compose the two
  deployments. Public `module` and `remote` packages are compatibility facades.
- `internal/infrastructure/persistence/database/{dataexchange,schema}` owns DML
  and migration definitions; concrete dialect profiles remain under
  `sqlite`, `mysql`, and `postgres`.
- `internal/transport/http/module` owns the deployment-neutral product Job HTTP
  Adapter used by both Module and the thin SaaS binding wrapper;
  `internal/transport/http/saas` remains reserved for the standalone remote
  server protocol.

There is no placeholder `cmd/data-exchange-server`: the current SaaS SDK
contract is a client transport contract. A standalone process should be added
only together with a real server-side transport/host contract.
