# Domainry Data Exchange

Data Exchange owns the engineering mechanics of large tabular import/export. Runtime applications provide authorized domain operations through the SDK Provider interfaces; they do not own upload chunks, worker checkpoints, generated artifacts, or file parsing loops.

The deployment-neutral SDK owns the single bounded CSV codec. The durable
Module owns the only import orchestration and chunk persistence path; Runtime
does not import implementation packages or maintain a second file engine.

## Deployments

- `module.Factory` runs in-process and owns `_data_exchange_jobs`, `_data_exchange_job_chunks`, `_data_exchange_artifacts`, and the workspace queue-scope index in the host database.
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
- Processing failures retry three times with bounded exponential backoff before
  the job enters its terminal failed/dead-letter state.
- Workspace queue discovery is separated from workspace-scoped job access for PostgreSQL RLS compatibility.
- Artifact downloads stream ordered result chunks.

White-label presentation is intentionally outside this module.

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
- `internal/transport/http/{module,saas}` reserves transport ownership without
  coupling domain or application packages to HTTP.

There is no placeholder `cmd/data-exchange-server`: the current SaaS SDK
contract is a client transport contract. A standalone process should be added
only together with a real server-side transport/host contract.
