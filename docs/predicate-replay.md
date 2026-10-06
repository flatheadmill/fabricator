# Predicate replay

Replay compares recorded Intersects cases with an exhaustive geometry reference and two explicit engine builds:

```sh
predicate replay cases/intersects/corpus.json \
    --workers workers.json --output runs/example --timeout-seconds 60
```

The input can also be a single case. The output directory must not exist. Replay does not build executables or discover an engine checkout.

## Inputs

A corpus fixes case order and resolves paths relative to its own directory:

```json
{
  "version": 1,
  "cases": [
    "plane_later_member_crossing.json",
    "sphere_later_member_crossing.json"
  ]
}
```

The worker manifest has exactly three roles: `reference`, `baseline` and `repaired`. Each role contains these fields:

```json
{
  "executable": "build/predicate",
  "executable_sha256": "<64 lowercase hexadecimal digits>",
  "build_record": "build/record.json",
  "build_record_sha256": "<64 lowercase hexadecimal digits>",
  "engine_revision": "<40 lowercase hexadecimal digits>"
}
```

The enclosing object also has `"version": 1`. Paths resolve relative to the worker manifest. Digests identify file bytes, including whitespace in JSON. Build records must have version 1 and the matching `engine_revision`; their other fields describe the controlled build. An executable digest alone is not source provenance.

The running parent must match the reference role's executable digest and compiled identity. Reference may explicitly repeat the repaired entry. Baseline and repaired must have different executable digests and different engine revisions. There is no implicit current-executable worker. Unstamped binaries fail preflight.

Replay preserves executable copies and build records, verifies the copies and runs their identity operation before launching cases. Every declared source path is checked, including roles that share executable bytes. One preserved copy of each executable serves the entire run. The current executable path identifies a file; hashing it does not independently prove the identity of the already loaded image. Keep executables, manifests and case inputs unchanged during a run.

## Observations

Original input, manifest and case bytes are preserved before evaluation. Numbered case directories retain corpus order. The parent evaluates the raw reference once per parsed case. Invalid, unsupported, ambiguous, malformed, unreadable and failed inputs remain explicit outcomes. A reference answer that differs from the recorded expectation also skips the engines. A judged empty answer still runs both workers.

Each worker gets a fresh index and its own process. Arguments and paths are recorded. The child inherits the environment, receives no stdin, and writes stdout and stderr to files. The positive timeout applies separately to every identity and worker invocation; its default is 60 seconds. On timeout or a supervision error, the parent kills and waits for the child. This covers the child only; predicate workers spawn no other processes.

`report.json` keeps process status, protocol issues, semantic comparison and diagnostic admission separate. A semantic comparison requires exit zero, matching requested and compiled identities, the observed case digest, the final result stage, and unique known IDs in sorted order. It reports both missing and spurious IDs. A known ID excluded by the filter is a spurious hit.

Invalid or absent required diagnostics prevent agreement without discarding an independently valid comparison. Diagnostics are not required before the query stage on a failed setup. Noisy stdout also prevents agreement; its first 4096 bytes are included as an excerpt, with the full bytes retained separately. A result file after a timeout, nonzero exit or incomplete stage is never compared.

Panic evidence requires exit 101 and a standard Rust panic line. The report retains the thread, source location, message, last admitted stage and diagnostic facts. That evidence describes the observed failure; it does not by itself establish a particular historical defect. The raw files remain available when records fail admission.

## Exit status

- `0`: Every supplied case has an outcome and every launched worker has an observation. Engine disagreements, panics and timeouts can occur in this report.
- `1`: Invocation, preflight or orchestration failed. When the output directory could be created, the report records the failure and any earlier observations.
- `2`: The run completed, but the reference itself failed. Read the reference failures in the report.

Exit zero does not mean that the engines agree. Read the per-worker `agreement` fields. Historical calibration is a separate judgment.

The parent catches unwinding panics only around reference evaluation. Worker search panics remain uncaught, so their process exits and stderr can be examined. The [worker protocol](predicate-worker.md) describes the fresh-index route and its records.
