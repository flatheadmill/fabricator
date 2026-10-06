# Predicate worker protocol

The predicate binary can run a recorded Intersects case in a separate process.
It creates one fresh index through normal ingestion, checks the stored document
IDs, and records setup facts before constructing or running the query. It does
not evaluate the reference answer or skip a query with an empty filter.

`predicate identity` prints the protocol version and compiled build-record
SHA-256 and engine revision. Missing stamps are reported as null. A worker
requires both stamps and checks them against its requested identity. The build
sets PREDICATE_BUILD_RECORD_SHA256 and PREDICATE_ENGINE_REVISION at compile time;
setting them in the worker's runtime environment has no effect.
Both stamps use lowercase hexadecimal: 64 digits for the build-record digest
and 40 for the engine revision.

The internal worker command takes explicit inputs:

```sh
predicate worker --case case.json --case-sha256 <digest> \
    --build-record-sha256 <digest> --engine-revision <revision> \
    --output run/worker
```

The caller creates run/ and captures stdout and stderr there. The worker creates
run/worker itself and refuses an existing directory. Worker stdout is empty.
The identity command alone writes JSON to stdout.

The first stage record separates requested values from the compiled identity
and records argv. The observed case digest is absent until the worker reads the
case. It reads and hashes those bytes once, checks identity and digest, then
parses and admits the case. The identity_verified field concerns those bindings,
not geometric validity or the semantic answer.

The stages are validate, index, diagnostics, query and result. A stage names work
in progress. Stage files are replaced atomically. Normal errors attempt to write
error.json and exit nonzero, with the reason on stderr. If recording the error
also fails, stderr includes both failures. Panics use the ordinary Rust handler.
An absent error record does not identify the cause of an exit.

The worker uses one writer thread, one commit and NoMergePolicy. After reopening
the index, it requires one physical segment, no deletions, and exactly the input
set of unique stored IDs. Filters are translated through this recovered mapping;
document insertion order is not used as an identity lookup. None passes no mask;
All, Include and Exclude pass an explicit mask, even when empty.

Before query execution, diagnostics.json records identity and case digest,
segment and document counts, stored IDs, selected IDs, filter and sidecar state.
It also records diagnostic clipper options and the query cells' overlapping
index ranges, levels, member IDs and edge bounds. These are recomputed setup
facts, not a traversal trace. Intersects builds its own query index.
An empty index range has a null first_index_level, not a level-zero sentinel.

A successful worker writes result.json with the verified identity, case digest
and sorted actual IDs, then enters the result stage and exits zero. A caller
must check all three before accepting completion. A result file alone does not
prove the worker completed.

Fresh-segment ingestion currently leaves the cell doc-ID sidecar empty. The
recorded cases request sidecar=false. A true request with no sidecar is a setup
error before query execution. The worker does not create replacement component
data or switch ingestion routes.

Run `cargo test --test predicate_worker` for ingestion, ID recovery, filter,
identity-admission and record-writing tests. Those tests use synthetic stamps
and do not establish the source identity of a calibration build.
