# Recorded predicate builds

`scripts/build_predicate.py` uses Python 3.11 or later and a native Rust toolchain. It builds the `predicate` binary in release mode with default features. It does not create worktrees, switch branches, fetch dependencies or run calibration.

Give each engine revision its own sibling checkout pair and target directory. Fabricator's path dependencies expect the engine beside the harness:

```text
baseline/fabricator
baseline/tantivy
repaired/fabricator
repaired/tantivy
```

Use the same harness source and build configuration for both pairs. Each engine worktree must be clean and at the exact requested commit. Build outputs must be outside the engine worktree. The output directory must be new; the target directory may be reused for repeated builds of that pair.

```sh
python3 scripts/build_predicate.py \
    --harness /work/repaired/fabricator \
    --engine /work/repaired/tantivy \
    --engine-revision <full-commit> \
    --target-dir /work/targets/repaired \
    --output /work/builds/repaired
```

Add `--offline` when the required dependencies are already cached. The helper runs Cargo metadata from the harness directory and verifies the resolved Tantivy and tantivy-common manifest paths. Other local dependencies must belong to the engine worktree. Cargo uses the existing lockfile with `--locked`.

The build record includes the clean engine revision, harness commit, tracked source diff, tracked and untracked Rust source hashes, Cargo manifests and lockfile, resolved package graph, compiler and Cargo versions, target, profile, helper digest, build command and selected build environment. It excludes ordinary spatial input data. Cargo configuration is recorded by path and digest without copying private file contents. This is evidence for a local controlled build under unchanged inputs, not a hermetic build claim.

The helper checks Cargo configuration in the harness directory, its ancestors and CARGO_HOME. It records both config filenames and honors the legacy `config` file's precedence over `config.toml`. Active included config, forced environment, profile, target, path, patch, unstable settings and source replacement are refused. Compiler, wrapper, flags, target-directory, build-directory and incremental overrides are refused in active build config and their supported environment forms. Compiler bootstrap, unstable Cargo environment settings and ambient identity stamps are also refused. Error messages name refused environment variables without printing their values. Native compiler settings such as CC and CFLAGS are recorded; arbitrary native-tool behavior is outside this procedure's guarantee.

Source capture covers Rust files, Cargo manifests and lockfiles, and toolchain files. If the binary gains non-Rust files embedded with `include_str!` or `include_bytes!`, add those files to the recorded source inventory.

`record.json` is written before compilation. Its SHA-256 digest and the engine revision become `PREDICATE_BUILD_RECORD_SHA256` and `PREDICATE_ENGINE_REVISION` for that build. After compilation, the helper checks that source and configuration inputs stayed unchanged, copies the executable, verifies its identity operation, hashes the executable and writes `entry.json`. The executable hash is outside the stamped record to avoid a circular digest.

Retain the whole output directory. It contains the helper copy, build record, build stdout/stderr, identity stdout/stderr, executable and manifest entry. Assemble the three-role replay manifest from explicit entries:

```python
import json
from pathlib import Path

baseline = json.loads(Path('/work/builds/baseline/entry.json').read_text())
repaired = json.loads(Path('/work/builds/repaired/entry.json').read_text())
manifest = {'version': 1, 'reference': repaired, 'baseline': baseline, 'repaired': repaired}
Path('/work/workers.json').write_text(json.dumps(manifest, indent=2) + '\n')
```

Run replay with the executable named as reference. The [replay contract](predicate-replay.md) explains input preservation, worker admission and exit codes.

## Stamp change check

Before relying on the procedure, run the helper twice against unchanged source and the same target directory, with two fresh output directories. The recorded output and invocation differ, so the records have different digests. Require both identity responses to match their own records, require different compiled record stamps, and compare the recorded source inputs to confirm they did not change. Preserve both builds and identity responses. This checks Cargo's handling of changed `option_env!` inputs with a reused target directory; it is a procedure check rather than a Cargo unit test.

The helper's focused tests run with:

```sh
python3 -B -m unittest discover -s tests -p predicate_build.py
```
