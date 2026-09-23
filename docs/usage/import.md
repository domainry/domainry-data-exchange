# How should a large import divide transfer work from row validation?

## Problems solved

- Separates reliable file transfer and job recovery from the business owner's row validation, authorization, and record writes.

## Business scenarios

- Migrating 100,000 customers while reporting malformed files, duplicate business numbers, and accepted-row progress.
- Loading a supplier catalog or enrollment roster where each valid row still obeys the owning domain's invariants.
- Cancelling a running import after some owner batches committed, without pretending those durable writes were rolled back.
- Resuming an expired worker lease from the last committed checkpoint while a corrected source is submitted as a new idempotent job.

## Use when

Use a durable import job when uploads are large, processing is asynchronous, retries are expected, or users need progress/cancellation/rejected-row evidence.

## Do not use when

Do not move parsing into the browser or create a project worker. Do not use a job for a small atomic request.

## How to use

Data Exchange validates transfer shape, stores chunks/checkpoints, and drives batches. The business owner validates fields, duplicates, permissions, and writes accepted rows through its typed capability.

## Adaptation cookbook

| Import scenario | Adapt with | Concrete implementation | Wrong adaptation |
| --- | --- | --- | --- |
| Customer migration contains malformed bytes and duplicate business numbers. | Transfer validation plus customer-domain validation | Data Exchange rejects malformed transfer data; the customer capability rejects duplicate rows and reports durable per-row evidence. | Treating every rejected row as a transport failure or letting Data Exchange decide customer uniqueness. |
| Supplier catalog import is cancelled halfway. | Durable job cancellation and checkpoints | Stop claiming new chunks, preserve completed/rejected counts, and expose a terminal cancellation result. | Deleting the upload while workers continue or pretending already committed rows rolled back globally. |
| Ten rows fit one atomic command. | Bounded Business Operation | Validate and commit synchronously when that matches accepted atomic semantics. | Creating an asynchronous job solely because the input arrived as CSV. |
| Worker lease expires after a committed checkpoint | Internal same-job resume with fencing | A new worker claims the job, verifies the source identity, and continues from the last committed cursor without replaying earlier chunks | Letting two workers apply the same chunk or creating a user-visible “retry” with a new source identity |
| User fixes rejected rows | New import submission | Preserve the failed job and row evidence; upload the corrected canonical source and submit a new idempotency identity | Mutating the original artifact/checkpoint or claiming a failed job resumed with different bytes |

## Example

A customer CSV Artifact records its hash and byte/chunk identity before job execution. Data Exchange validates the complete source before any apply batch runs; malformed CSV or owner validation rejections fail before application. During apply, every committed checkpoint records processed/rejected counts under a fenced lease. Cancellation stops future chunk claims but does not promise rollback of owner batches already committed. An expired worker lease resumes the same job from its last committed cursor; correcting source rows creates a new job rather than rewriting the original evidence. Final state includes source hash, checkpoint, totals, rejection/error evidence, operator, and terminal outcome; this is a runtime job protocol, not project Model JSON.

## Permissions and scope

The importer needs exact import permission and a truthful target scope. A file cannot smuggle arbitrary Workspace or organization identifiers past Runtime authorization.

## Boundaries

Data Exchange never becomes the customer source of truth and never bypasses the owning Handler’s validation.
