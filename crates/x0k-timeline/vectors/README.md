# The timeline conformance vectors

These files are the conformance suite for the timeline's public surface
(`corpora/x0k/decisions/architecture/substrate/timeline-public-surface.md`).
Any implementation of the surface runs the same files; x0k's run is
`tests/conformance.rs`.

Each file holds the vectors for one check of one clause:

- `clause`: the section of the decision the vectors hold to;
- `check`: what an implementation does with each vector's input;
- `about`: the claim, and what the input and the answer mean;
- `vectors`: each a `name`, an `input` and the `expect`ed answer.

Records are written in the published record format (§1): `entry_kind`,
`id`, `timestamp_us`, the other core fields, `fields` and `attributes`.

An expectation matches an answer when every field it names matches: an
object by its named keys (others are ignored), an array element by element,
`null` when the field is absent or null, and `{"$contains": "text"}` a
string containing the text.

| File | Check | Clause |
|---|---|---|
| `record.json` | `record.fit`: whether one record fits the record shape, and if not, which field | §1 |
| `ingest.json` | `ingest.batch`: what the writer accepts and refuses of a batch, and what reaches its store | §1, §2 |
| `ingest-open.json` | `ingest.open`: whether a session opens, given the two ends' record shapes | §2 |
| `otlp.json` | `otlp.map`: one OTLP/HTTP request to the writer, its answer and the records it writes | §2 |
| `segment.json` | `segment.read`: the records read back from a segment whose end may be torn | §3 |
| `reader.json` | `reader.filter`: what the base reader answers for a filter | §4 |
| `families.json` | `families.fold`: what `describe_timeline` says of a family, from its rows | §4 |
| `pause.json` | `writer.pause`: what the writer answers and keeps as it pauses, resumes and restarts | §7 |
| `state-history.json` | `state.fold`: a journal's state at a sequence, or the gap that stops the fold | §5 |

An implementation that does not yet hold a check or a vector says so by
name, never by passing it.
