# The timeline's public surface

```turtle folio:document
architecture:timeline-public-surface a x0k:Architecture ;
    x0k:status "accepted" ;
    x0k:summary "The published timeline (x0k-timeline) promises a record shape, an ingest operation in canonical CBOR frames, a crash-durable segment format, a base reader, a SQL surface, a state-history row, a Chrome trace export and its MCP tool names, each versioned by shape fingerprint. The record has core fields every user shares and namespaced attributes for each user's own; entry kinds are an open enum. A process that cannot link the emit role sends OTLP on localhost. Fleet placement, the entry store and x0k's own column groups sit behind the surface. The data model, its codec and the shape language reach the publication as one standalone crate." ;
    x0k:concerns "timeline", "observability", "interfaces", "versioning", "shapes", "wire", "publication" ;
    x0k:motivatedBy architecture:publication-is-the-shipping-unit,
        design:publish-a-region-as-a-repository,
        intent:f0cf7d02-e8d9-d524-d623-03fed7f91eac ;
    x0k:mentions architecture:observability-timeline,
        architecture:value-shapes,
        architecture:operation-surface,
        architecture:core-and-edges,
        architecture:execution-journal,
        architecture:graded-introspection,
        architecture:entry-based-substrate,
        architecture:plan-vm-public-surface .
```

## Brief

The timeline's local tier is published on its own as `x0k-timeline`. A
developer who adopts it gets one record format for spans, logs and
activities from every process on a machine, a crash-durable file each
process writes when the writer is gone, one SQL surface and one set of
tool names over all of it, and a row for an application's own state
history that the same query reads. A user can pause the writer while it
runs, and the timeline shows the gap. This decision fixes what that
publication promises, so a second user can write records in the published
shape from its first line of code and keep them readable across releases.

Every promise is a shape (`value-shapes`). A record that does not fit the
record shape is refused at ingest with the field named, and nothing is
coerced. A release names its surface by the fingerprints of its shapes, so
two ends know whether they agree before the first record crosses. An
interface grows by addition: a new field, a new attribute namespace or a
new entry kind leaves every earlier reader and every stored record valid.
The bytes on the wire, in a segment and in anything hashed are canonical
CBOR, the one spelling every x0k boundary uses.

x0k is one user behind this surface. Where its timeline differs (fleet
placement, record-class facts in the entry store, its own column groups),
that difference sits behind the surface and is never a dependency of it.

## Context

`observability-timeline` decides the substrate: one record set, the
retention matrix, a writer with a crash-durable segment per process, a base
reader that answers while the writer is down, a query service, and W3C
trace correlation. It does not decide which of those an outside user may
rely on. The practice this decision leaves is postcard for the forward
wire and the segment, an encoding
whose layout comes from the writer's Rust declaration order and which
decodes only by the writer's own type. `operation-surface` §1 rules that
out between processes: frames are canonical CBOR, opened by agreeing shape
fingerprints, and a cheaper form must derive from the shape
(`value-shapes` §7).

The first outside user is chariot, a developer tool with a Rust daemon, a
TypeScript agent sidecar and a web GUI. Its daemon writes typed state
events from one `apply()`, and its GUI store records and replays actions.
Both need the state-history row to be public before they write it.
`plan-vm-public-surface` is the pattern this decision follows: promise only
what is the component's own, version each promise, and keep x0k behind
interfaces.

## Decision

### 1. The record is a shape with core fields and namespaced attributes

**Decides:** what one timeline record contains and how it grows.
**Invariants:** every record fits the record shape; a record's identity is
its entry kind and its id; a namespace's attributes change only by that
namespace's owner.
**On failure:** ingest refuses a misfit record and names the field; the
other records of the batch are kept.
**Checked by:** the timeline conformance suite (intent 56421f3b).

The record shape is a struct whose variant is spelled `inline` under the tag
key `entry_kind` (`value-shapes` §1). The core entry kinds are `span`, `log`
and `activity`. The enum is open: a user adds a kind (x0k adds
`conversation` and its ingested kinds, `observability-timeline` §10), and a
reader that does not know a kind keeps the record as foreign data.

Core fields are those every user needs to correlate and filter:

| Field | Meaning |
|---|---|
| `id` | Unique within its entry kind |
| `timestamp_us` | Microseconds since the Unix epoch |
| `node` | The node that wrote it, optional |
| `process` | Process id, the segment owner key, and the software module |
| `level`, `target`, `module` | The tracing level, target and module path |
| `source` | File, line and function, optional |
| `trace_id`, `span_id`, `parent_span_id`, `tracestate` | W3C trace context |
| `session_id` | The session the work ran in, optional |
| `event_type`, `description` | The semantic name and its text |
| `started_at_us`, `ended_at_us` | A span's or an activity's interval |
| `retention_class` | `span`, `log`, `event` or `record` |
| `fields` | The call site's own fields, by name |
| `attributes` | Namespaced attributes |

`fields` holds what the call site named; its schema is observed per event
family (`observability-timeline` §5). `attributes` holds what a user adds to
every record of its own, keyed `<namespace>.<name>`. The `x0k` namespace
holds x0k's column groups: authority context, derivation handles, and its
workstream, intent and seed ids (`provenance`). Another user's namespace,
such as `chariot`, holds its own. A namespace is never a core field, so no
user's fields enter the core by being useful to that user.

Stored records are open (`value-shapes` §1): a reader keeps fields it does
not know.

### 2. Ingest is an operation in canonical CBOR frames

**Decides:** how a process hands records to the writer.
**Invariants:** what is hashed, journalled or written to a segment is
canonical CBOR; a session agrees on shape fingerprints before its first
record.
**On failure:** a session whose two ends hold incompatible record shapes is
refused at open, by name; the client appends to its segment (§3).
**Checked by:** the timeline conformance suite (intent 56421f3b).

The writer serves `timeline.ingest`, whose argument is a batch of records
and whose answer counts what it accepted, refused and, while paused (§7),
discarded. It travels as an
operation of the writer's operation set over a local session
(`operation-surface` §1), in canonical CBOR frames. A session whose two
ends hold equal fingerprints for the operation may carry it in a form
meeting `value-shapes` §7. The trace-override operations the writer serves
(set an override, clear one, read the effective set) are part of the same
operation set and the same surface.

A writer may itself forward to an **upstream writer**: the same ingest
operation over a session to another machine. It keeps each record until the
upstream acknowledges it, keeps what arrives while the upstream is
unreachable, and sends what was not acknowledged when it returns, oldest
first. Which upstream a writer forwards to, and how it authenticates there,
is its embedder's: x0k names the holder of the collector role
(`observability-timeline` §15), and another user names an address, such as
a team's collector machine.

A process that cannot link the emit role sends OTLP over HTTP to the writer
on localhost. The writer maps each OTLP span, log record and event to a
record: the W3C identifiers to the core fields, the resource and scope to
`process`, and every other attribute to `attributes` under its own key. The
mapping is part of this surface. The `x0k.` prefix is reserved and never
taken from OTLP input.

### 3. The segment is the record, framed

**Decides:** the file a process writes while the writer is unreachable.
**Invariants:** a segment holds only whole records in canonical CBOR, in
the order written; its owner holds `{key}.lock` for its whole life; a
reader that has only the CBOR codec reads it.
**On failure:** a truncated trailing frame is dropped and the frames before
it are read; a full segment refuses new records and counts each refusal
(`observability-timeline` §4).
**Checked by:** the timeline conformance suite (intent 56421f3b);
`observability-timeline` §4's cost law check.

A segment starts with a header frame naming the record shape's fingerprint,
then holds length-framed records. Segments live in the profile's segment
directory under the owner key that names the process. The writer takes in
a segment whose owner no longer holds its lock. A reader of a different
release reads the header first and reads the records through its own shape
(`value-shapes` §6).

### 4. Reading has two layers and one set of names

**Decides:** what a user reads with, and under which names.
**Invariants:** a query that names only core fields runs on every build; a
record that has reached both a segment and the store is counted once.
**On failure:** a reader that cannot reach the store answers from segments
alone and says so.
**Checked by:** the timeline conformance suite (intent 56421f3b).

The **base reader** decodes segments with the CBOR codec alone and filters
by time range, node, process, session, trace, level and target. The
**query service** adds SQL over the store and the segments together. Its
table `timeline` has a column per core field and the attributes as one map
column; a build may add typed columns for its own namespaces, and x0k's
build carries its column groups that way. Each event family is a view
(`observability-timeline` §5). `json_extract` remains for free-form text.

The MCP tools and the CLI are generated from one operation set:
`query_timeline_sql`, `describe_timeline`, `query_call_stack`,
`export_chrome_trace`, `pause_timeline` and `resume_timeline`. `export_chrome_trace` turns a time window, a trace or
a session into Chrome trace JSON for Perfetto: a track per process, a slice
per span, an instant per event, and flows for parent links across
processes.

### 5. An application's state history is a row the same query reads

**Decides:** how an application's own record of its state joins the
timeline.
**Invariants:** a state at a sequence is the latest snapshot at or before
it with every delta after it applied in order; every row names its journal
and sequence.
**On failure:** a gap in a journal's sequence is reported by the reader;
it never folds across one.
**Checked by:** the timeline conformance suite (intent 56421f3b).

A **state-history row** carries `journal`, `seq`, `timestamp_us`, the named
action or event, `trace_id` and `span_id`, and the delta it produced. A
**snapshot row** carries `journal`, `seq` and the whole state. Each
journal's state and delta have shapes of the journal owner's, named by
fingerprint on the journal's first row. The query service registers each
journal as a table beside `timeline` (`observability-timeline` §13), so one
query joins an action to the spans it caused through `trace_id`. A daemon's
typed state events and a GUI store's actions are journals of this kind.

### 6. x0k sits behind the surface

**Decides:** what the publication does not promise.
**Invariants:** the published crates build with no x0k crate beyond those
the publication ships.
**On failure:** not stated.
**Checked by:** the publication's build from a clean clone.

Behind the surface, and never a dependency of it: fleet placement
(`observability-timeline` §15, `durability-policy`,
`data-interest-and-topology`), how a collector is chosen and replaced, the entry store, the `x0k` attribute namespace's meaning,
the `conversation` kind and its query-time join, the ingesters of oracle
verdicts, and x0k's process supervision of the writer. The supervisor's
observations are public in shape (§8), and how x0k produces them is not.

The writer hands record-class rows to a record store when its embedder
provides one; x0k provides the entry store, so in x0k they are facts every
node holds (`observability-timeline` §15). With no record store the writer
keeps them in its own store under the record retention. The publication
depends on no `x0k-core` crate and no entry store.

x0k's `activity.enabled` setting reaches the writer through `timeline.pause`
and `timeline.resume` (§7). The setting and whatever watches it are x0k's.

The data model, its canonical CBOR codec and the shape language reach the
publication as one crate that holds only them, which the timeline, the
UI-control protocol and the sync core publications share (intent
a214befd). The runtime contract is not a dependency of the publication.

### 7. The writer can be paused while it runs

**Decides:** how a user stops the timeline keeping records, and what stays
true while it is stopped.
**Invariants:** every pause and resume is a record-class row, so a query
can tell a paused window from a quiet one; a paused writer keeps every
record-class row; a client sees no failure from a pause; a pause lasts no
longer than the writer that took it.
**On failure:** a pause whose row cannot be written is refused by name, and
the writer keeps recording.
**Checked by:** the timeline conformance suite (intent 56421f3b).

The writer serves `timeline.pause`, whose argument says why, and
`timeline.resume`. Both are operations of the writer's operation set (§2).
A pause is local to one writer. `describe_timeline` answers whether the
writer is paused, since when and why.

While paused, the writer goes on taking records, so a client neither fails
nor falls back to its segment. It discards span, log and event rows, counts
them by class, and reports the counts in the ingest answer and on the
resume row. It keeps record-class rows, because a user relies on those
being there. Discarded rows are never forwarded upstream. Segments left by
processes that have exited stay on disk, and the writer takes them in after
it resumes.

An embedder can start the writer paused. A restart returns the writer to
the state its embedder starts it in, so a pause taken by hand does not
outlive the writer.

### 8. A process's life is observed in the same record

**Decides:** how something watching a process records what the process
cannot say about itself.
**Invariants:** an observation is a `log` record whose `event_type` names it,
so anything that speaks OTLP can write one; the record's `process` is the
observer and its attributes name the subject, so a reader can always tell an
observation from an application's claim about itself; an observer that could
not know whether an OOM kill ended a process says `unknown`, never `no`; an
observation is kept as an `event`, not a `log`.
**On failure:** a record named as an observation that is not one is refused
by name, and the other records of its batch are kept.
**Checked by:** `otlp.json` in the timeline conformance suite.

A process that fails to start, is OOM-killed or panics after its subscriber
has gone cannot report it, and silence reads as nothing having happened. So
any observer can report it instead: a wrapper, a service manager's hook or a
supervisor. The events are the lifecycle grammar of `platform-supervision`
§7.

| `event_type` | Attributes it must carry |
|---|---|
| `process.spawned`, `process.adopted` | `process.pid` |
| `process.exit` | `process.pid`, `timeline.process.oom` (`yes`, `no`, `unknown`), `timeline.process.cause` (`exited` with `process.exit.code`, `signalled` with `timeline.process.signal`, or `unknown`) |
| `process.abandoned` | `process.pid`, `timeline.process.bound_ms` |

Attributes use OpenTelemetry's `process.*` names where it has them, such as
`process.command`. It has none for a signal, an OOM kill or a cause, and a
namespace changes only by its owner (§1), so those are `timeline.process.*`.

An observation is a log record and not a new entry kind because OTLP, the
ingest any process can reach, cannot name a kind: the event name would decide
it either way, and a kind would ask every observer for more than a log.

Its class is `event` because evidence of a death has to outlast the logs
around it. Retention deletes by file, so a batch of log records is split by
class before it is written.

## Rationale

- **Shapes make the contract checkable in any language.** A TypeScript
  sidecar can validate a record against the published shape before it
  sends it. A postcard layout is a Rust declaration a second user can only
  imitate.
- **Canonical CBOR is the spelling x0k already commits every boundary to**
  (`core-and-edges` §1). A second encoding for the timeline would be the
  one boundary a shape could not describe.
- **Namespaced attributes keep the core small.** The core is what
  correlation and filtering need. A user's own fields grow in its namespace
  without a core release, and x0k's 58 columns stop being everyone's schema.
- **A pause is an operation because a config file would be x0k's.** Any
  user can call an operation, from a tool or from an agent. Watching a file
  would make x0k's layered configuration part of the surface.
- **A gap has to be visible.** A timeline is evidence, and a stretch with no
  rows reads as nothing having happened unless the pause is itself a row.
- **The state-history row is public before chariot writes it.** A row
  format fixed after its first writer ships would need a migration on day
  one.

## Consequences

### Positive

- A second user writes records in the published shape from its first line
  and keeps them readable across releases of the same major version.
- A process in any language reaches the timeline, by the shape over a
  session or by OTLP on localhost.
- x0k's fleet placement and entry-store facts change without a release of
  the publication.

### Negative

- x0k's forward wire and segment change encoding (intent a265f9fe). The
  timeline is x0k's highest-rate local path, so the change is measured
  before it is made (`operation-surface` § "A change of encoding is
  measured before it is made").
- The record shape, the ingest operation, the segment format and the
  state-history row carry compatibility obligations. A change that is not
  additive needs a major version.
- `pause_timeline` is the first tool in the set that changes the writer
  rather than reading it, so it carries a side-effect class the query tools
  do not.
- x0k needs a process to forward `activity.enabled` to the writer. Until
  now a change reached the writer through the config watcher in its own
  process.
- The publication waits on the data model, codec and shape language
  standing alone (intent a214befd).

## Alternatives Considered

- **Postcard as the published wire.** Rejected: its layout is a Rust
  declaration's order, it decodes only by the writer's type, and
  `operation-surface` §1 refuses it between processes.
- **OTLP as the native wire.** Rejected: it has no place for the segment's
  crash durability or for x0k's identity-preserving record, and it is
  interop, which §2 provides as an input.
- **`activity.enabled` read from x0k's configuration by the writer.**
  Rejected: it ties the published service to x0k's configuration, and §6
  keeps that behind the surface.
- **A pause that drops everything.** Rejected: x0k's flag has done this, and
  it would silently lose the record class, whose rows are kept for a user to
  rely on.
- **x0k's 58 columns as the core.** Rejected: most are x0k's own column
  groups, and a second user would carry them all.

## Open Questions

- How x0k's present column names map to core fields plus the `x0k`
  namespace without breaking the timeline parity corpus's queries.
- Whether the base reader's filters are an operation of their own, so a
  remote reader can call them, or stay a library API.

## Related Decisions

- [observability-timeline](observability-timeline.md): the substrate this
  surface publishes a part of; its §4 segment and §9 roles take their
  encoding from §2 and §3 here.
- [value-shapes](../surfaces/value-shapes.md): what a shape is, how it is
  checked, identified and evolved.
- [operation-surface](../platform/operation-surface.md): operations in
  canonical CBOR frames, sessions opened by shape agreement, and measuring
  an encoding change first.
- [core-and-edges](../surfaces/core-and-edges.md): the data model and its
  canonical spelling.
- [execution-journal](../surfaces/execution-journal.md) and
  [graded-introspection](graded-introspection.md): journals the query reads
  beside the timeline.
- [entry-based-substrate](entry-based-substrate.md): where x0k holds
  record-class rows, behind the surface.
- [plan-vm-public-surface](../surfaces/plan-vm-public-surface.md): the
  pattern this decision follows.
