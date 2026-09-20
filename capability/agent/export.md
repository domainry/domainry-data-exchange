# When should an online query become a durable export artifact?

## Problems solved

- Turns a large authorized result into a durable downloadable artifact without holding one HTTP request open or widening source access.

## Business scenarios

- Exporting two million customer records for an approved migration.
- Preparing a large finance or compliance file with stable paging, progress, cancellation, and download evidence.

## Use when

Use a durable export for large/offline results, progress, cancellation, retry, retention, or evidence requirements.

## Do not use when

Do not create export lifecycle for one bounded online page. Do not allow the browser to crawl all pages and assemble a file.

## How to use

The source owner or Report provides stable authorized pages/canonical bytes. Data Exchange owns the job, chunks, artifact, progress, and download lifecycle.

## Adaptation cookbook

| Export scenario | Adapt with | Concrete implementation | Wrong adaptation |
| --- | --- | --- | --- |
| Export two million customers for an approved migration. | Object/source paging plus Data Exchange artifact | Freeze the authorized source contract, page by stable cursor, expose job progress/cancel, and grant artifact download separately. | Browser-crawling every page or granting export because read is allowed. |
| Export monthly revenue by region. | Report export preparation plus Data Exchange | Report owns measures, dimensions, parameters, and data scope; Data Exchange owns chunks and the downloadable CSV. | Recomputing aggregates inside the export worker. |
| Export immutable change evidence. | Audit export plus Data Exchange only where artifact mechanics are needed | Audit owns evidence selection and reviewer scope; Data Exchange may deliver the resulting file. | Using a business Object query as a substitute for audit history. |

## Example

A Workspace administrator exports two million customer records for an approved migration. The customer owner supplies stable authorized pages; Data Exchange builds the durable artifact and exposes progress, cancellation, and download evidence.

## Permissions and scope

Export and artifact download are separate permissions. The generated artifact preserves the requesting principal’s Workspace and row scope.

## Boundaries

Report owns analytics, source Objects own transactional records, and Data Exchange owns offline transfer—not unrestricted query semantics.
