# Domainry Data Exchange development guide

- This repository is an independent Go module and must not import `domainry-runtime/internal/**` or Runtime implementation packages.
- Preserve the internal DDD layout: `internal/application/dataexchange`, `internal/domain/dataexchange/{model,repository,service}`, `internal/adapter`, `internal/assembly`, `internal/transport`, and `internal/infrastructure`.
- Data Exchange owns durable jobs, chunks, artifacts, worker recovery, and import/export orchestration.
- Runtime supplies authorized providers through the public SDK host contracts; Data Exchange must not import Runtime domain models.
- Embedded Module persistence uses the host database, dialect, transaction boundary, migration lock, and the host-owned `_schema_migrations` ledger.
- Keep public deployment packages as thin facades over `internal/assembly`; do not expose application, domain, persistence, or transport implementations.
