#!/usr/bin/env python3
"""Build a recorded predicate worker with a fixed release configuration."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tomllib


PROTOCOL_VERSION = 1
STAMP_KEYS = {"PREDICATE_BUILD_RECORD_SHA256", "PREDICATE_ENGINE_REVISION"}
FORBIDDEN_ENV = STAMP_KEYS | {
    "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CARGO_BUILD_RUSTFLAGS",
    "RUSTC", "RUSTC_BOOTSTRAP", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER",
    "CARGO_BUILD_RUSTC", "CARGO_BUILD_RUSTC_WRAPPER",
    "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER", "CARGO_BUILD_TARGET",
    "CARGO_TARGET_DIR", "CARGO_BUILD_TARGET_DIR", "CARGO_BUILD_BUILD_DIR",
    "CARGO_INCREMENTAL", "CARGO_BUILD_INCREMENTAL",
}
FORBIDDEN_BUILD = {
    "rustc", "rustc-wrapper", "rustc-workspace-wrapper", "rustflags",
    "target", "target-dir", "build-dir", "incremental",
}
RECORDED_ENV = {
    "PATH", "CARGO_HOME", "RUSTUP_HOME", "RUSTUP_TOOLCHAIN", "CARGO_BUILD_JOBS",
    "CC", "CXX", "AR", "CFLAGS", "CXXFLAGS", "CPPFLAGS", "LDFLAGS",
    "SDKROOT", "MACOSX_DEPLOYMENT_TARGET", "SOURCE_DATE_EPOCH",
}


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def json_bytes(value):
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


def write_json(path, value):
    data = json_bytes(value)
    with path.open("xb") as stream:
        stream.write(data)
    return data


def run(argv, cwd):
    result = subprocess.run(argv, cwd=cwd, capture_output=True)
    if result.returncode:
        raise RuntimeError(f"{argv[0]} failed: {result.stderr.decode(errors='replace').strip()}")
    return result.stdout


def check_environment(environment):
    refused = sorted(key for key in environment if (
        key in FORBIDDEN_ENV or key.startswith(("CARGO_PROFILE_", "CARGO_UNSTABLE_"))
        or (key.startswith("CARGO_TARGET_") and key.endswith(("_RUSTFLAGS", "_LINKER")))
    ))
    if refused:
        raise ValueError("unsupported ambient build settings: " + ", ".join(refused))


def check_config(config):
    refused = sorted(set(config) & {"include", "env", "profile", "target", "paths", "patch", "unstable", "source"})
    refused.extend("build." + key for key in sorted(set(config.get("build", {})) & FORBIDDEN_BUILD))
    if refused:
        raise ValueError("unsupported Cargo configuration: " + ", ".join(refused))


def configuration(harness, cargo_home):
    directories = [harness / ".cargo"]
    directories.extend(parent / ".cargo" for parent in harness.parents)
    directories.append(cargo_home)
    records = []
    for directory in dict.fromkeys(directories):
        legacy = directory / "config"
        modern = directory / "config.toml"
        active = legacy if legacy.is_file() else modern
        for path in (legacy, modern):
            if not path.is_file():
                continue
            data = path.read_bytes()
            records.append({"path": str(path), "sha256": sha256(data), "active": path == active})
            if path == active:
                try:
                    check_config(tomllib.loads(data.decode()))
                except (ValueError, TypeError) as error:
                    raise ValueError(f"{path}: {error}") from error
    return records


def git(root, *args):
    return run(["git", *args], root)


def clean_engine(root, revision):
    if len(revision) != 40 or any(c not in "0123456789abcdef" for c in revision):
        raise ValueError("engine revision must be 40 lowercase hexadecimal digits")
    if Path(git(root, "rev-parse", "--show-toplevel").decode().strip()).resolve() != root:
        raise ValueError("engine must name its worktree root")
    if git(root, "rev-parse", "HEAD").decode().strip() != revision:
        raise ValueError("engine HEAD differs from the requested revision")
    if git(root, "status", "--porcelain=v1", "--untracked-files=all").strip():
        raise ValueError("engine worktree must be clean")


def harness_source(root):
    if Path(git(root, "rev-parse", "--show-toplevel").decode().strip()).resolve() != root:
        raise ValueError("harness must name its worktree root")
    names = git(root, "ls-files", "--cached", "--others", "--exclude-standard", "-z").decode().split("\0")
    names = sorted(set(name for name in names if name and (
        name.endswith(".rs") or Path(name).name in {"Cargo.toml", "Cargo.lock", "rust-toolchain", "rust-toolchain.toml"}
    )))
    files = [{"path": name, "sha256": sha256((root / name).read_bytes()) if (root / name).is_file() else None} for name in names]
    diff = git(root, "diff", "HEAD", "--binary", "--", *names).decode()
    return {
        "path": str(root), "revision": git(root, "rev-parse", "HEAD").decode().strip(),
        "source_files": files, "tracked_diff": diff,
    }


def verify_dependencies(metadata, harness, engine):
    for name, expected in (("fabricator", harness), ("tantivy", engine), ("tantivy-common", engine / "common")):
        packages = [package for package in metadata["packages"] if package["name"] == name]
        if len(packages) != 1 or Path(packages[0]["manifest_path"]).resolve().parent != expected:
            raise ValueError(f"resolved {name} manifest differs from {expected}")
    for package in metadata["packages"]:
        if package["source"] is None:
            parent = Path(package["manifest_path"]).resolve().parent
            if parent != harness and not parent.is_relative_to(engine):
                raise ValueError(f"unrecorded local dependency: {parent}")


def build(args):
    check_environment(os.environ)
    harness = args.harness.resolve(strict=True)
    engine = args.engine.resolve(strict=True)
    target = args.target_dir.resolve()
    output = args.output.resolve()
    if output.exists():
        raise ValueError("build output directory must not exist")
    if target.is_relative_to(engine) or output.is_relative_to(engine):
        raise ValueError("build outputs must be outside the engine worktree")
    clean_engine(engine, args.engine_revision)
    cargo_home = Path(os.environ.get("CARGO_HOME", Path.home() / ".cargo")).resolve()
    configs = configuration(harness, cargo_home)
    source = harness_source(harness)
    lock = (harness / "Cargo.lock").read_bytes()
    helper = Path(__file__).resolve()
    helper_bytes = helper.read_bytes()
    cargo = shutil.which("cargo")
    rustc = shutil.which("rustc")
    if not cargo or not rustc:
        raise ValueError("cargo and rustc must be on PATH")
    # Keep shim names intact; resolving a rustup symlink changes dispatch.
    cargo = str(Path(cargo).absolute())
    rustc = str(Path(rustc).absolute())
    rustc_version = run([rustc, "-Vv"], harness).decode().strip()
    host = next(line.removeprefix("host: ") for line in rustc_version.splitlines() if line.startswith("host: "))
    cargo_version = run([cargo, "-V"], harness).decode().strip()
    metadata_command = [cargo, "metadata", "--locked", "--format-version", "1"]
    command = [cargo, "build", "--locked", "--release", "--bin", "predicate", "--target-dir", str(target), "--message-format=json-render-diagnostics"]
    if args.offline:
        metadata_command.append("--offline")
        command.append("--offline")
    metadata = json.loads(run(metadata_command, harness))
    verify_dependencies(metadata, harness, engine)
    recorded_environment = {key: value for key, value in os.environ.items() if (
        key in RECORDED_ENV or key.startswith(("CC_", "CXX_", "AR_", "CFLAGS_", "CXXFLAGS_", "CPPFLAGS_"))
    )}
    record = {
        "version": 1, "protocol": PROTOCOL_VERSION,
        "engine_revision": args.engine_revision, "engine_path": str(engine),
        "harness": source, "lock_sha256": sha256(lock), "cargo_configuration": configs,
        "cargo_home": str(cargo_home), "metadata_command": metadata_command, "metadata": metadata,
        "cargo": {"path": cargo, "version": cargo_version},
        "rustc": {"path": rustc, "version": rustc_version},
        "target": host, "target_dir": str(target), "profile": "release", "features": "default",
        "helper_sha256": sha256(helper_bytes), "build_command": command,
        "environment": recorded_environment, "output": str(output),
        "invocation": {"argv": sys.argv, "cwd": str(Path.cwd())},
    }
    output.mkdir()
    record_digest = sha256(write_json(output / "record.json", record))
    (output / "builder.py").write_bytes(helper_bytes)
    environment = dict(os.environ)
    environment["PREDICATE_BUILD_RECORD_SHA256"] = record_digest
    environment["PREDICATE_ENGINE_REVISION"] = args.engine_revision
    with (output / "build.stdout").open("wb") as stdout, (output / "build.stderr").open("wb") as stderr:
        result = subprocess.run(command, cwd=harness, env=environment, stdout=stdout, stderr=stderr)
    if result.returncode:
        raise RuntimeError(f"build failed; see {output / 'build.stderr'}")
    clean_engine(engine, args.engine_revision)
    if source != harness_source(harness) or lock != (harness / "Cargo.lock").read_bytes():
        raise RuntimeError("harness inputs changed during the build")
    if configs != configuration(harness, cargo_home) or helper_bytes != helper.read_bytes():
        raise RuntimeError("build configuration or helper changed during the build")
    artifacts = []
    for line in (output / "build.stdout").read_bytes().splitlines():
        message = json.loads(line)
        if message.get("reason") == "compiler-artifact" and message["target"]["name"] == "predicate" and message.get("executable"):
            artifacts.append(Path(message["executable"]))
    if len(artifacts) != 1:
        raise RuntimeError("Cargo did not identify exactly one predicate executable")
    executable = output / "predicate"
    shutil.copy2(artifacts[0], executable)
    with (output / "identity.json").open("wb") as stdout, (output / "identity.stderr").open("wb") as stderr:
        result = subprocess.run([str(executable), "identity"], stdout=stdout, stderr=stderr, timeout=60)
    expected = {"protocol": PROTOCOL_VERSION, "build_record_sha256": record_digest, "engine_revision": args.engine_revision}
    if result.returncode or json.loads((output / "identity.json").read_bytes()) != expected:
        raise RuntimeError("compiled worker identity differs from the build record")
    entry = {
        "executable": str(executable), "executable_sha256": sha256(executable.read_bytes()),
        "build_record": str(output / "record.json"), "build_record_sha256": record_digest,
        "engine_revision": args.engine_revision,
    }
    write_json(output / "entry.json", entry)
    return output / "entry.json"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--harness", type=Path, required=True)
    parser.add_argument("--engine", type=Path, required=True)
    parser.add_argument("--engine-revision", required=True)
    parser.add_argument("--target-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--offline", action="store_true")
    args = parser.parse_args()
    try:
        print(build(args))
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired, StopIteration) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
