use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::case::Filter;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
pub const PROTOCOL_VERSION: u32 = 1;

pub fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn is_hex(value: &str, len: usize) -> bool {
    value.len() == len
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    pub protocol: u32,
    pub build_record_sha256: Option<String>,
    pub engine_revision: Option<String>,
}

impl Identity {
    pub fn compiled() -> Self {
        Self {
            protocol: PROTOCOL_VERSION,
            build_record_sha256: option_env!("PREDICATE_BUILD_RECORD_SHA256").map(str::to_owned),
            engine_revision: option_env!("PREDICATE_ENGINE_REVISION").map(str::to_owned),
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.protocol != PROTOCOL_VERSION {
            return Err("unsupported worker protocol".into());
        }
        if !self
            .build_record_sha256
            .as_deref()
            .is_some_and(|s| is_hex(s, 64))
            || !self
                .engine_revision
                .as_deref()
                .is_some_and(|s| is_hex(s, 40))
        {
            return Err(
                "worker requires a build-record digest and engine revision compiled into it".into(),
            );
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, clap::Args)]
#[serde(deny_unknown_fields)]
pub struct WorkerRequest {
    #[arg(long)]
    pub case: String,
    #[arg(long)]
    pub case_sha256: String,
    #[arg(long)]
    pub build_record_sha256: String,
    #[arg(long)]
    pub engine_revision: String,
    #[arg(long)]
    pub output: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Observed {
    pub case_sha256: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Context {
    pub requested: WorkerRequest,
    pub compiled: Identity,
    pub observed: Observed,
    pub identity_verified: Option<bool>,
    pub argv: Vec<String>,
}

impl Context {
    pub fn verify(&self) -> Result<Header> {
        self.compiled.validate()?;
        if self.compiled.build_record_sha256.as_deref() != Some(&self.requested.build_record_sha256)
            || self.compiled.engine_revision.as_deref() != Some(&self.requested.engine_revision)
        {
            return Err("requested worker identity differs from compiled identity".into());
        }
        let observed = self
            .observed
            .case_sha256
            .as_deref()
            .ok_or("case digest has not been observed")?;
        if !is_hex(observed, 64) || observed != self.requested.case_sha256 {
            return Err("observed case digest differs from requested digest".into());
        }
        Ok(Header {
            protocol: self.compiled.protocol,
            build_record_sha256: self.requested.build_record_sha256.clone(),
            engine_revision: self.requested.engine_revision.clone(),
            case_sha256: observed.into(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Header {
    pub protocol: u32,
    pub build_record_sha256: String,
    pub engine_revision: String,
    pub case_sha256: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    Validate,
    Index,
    Diagnostics,
    Query,
    Result,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Progress {
    pub context: Context,
    pub stage: Stage,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerError {
    pub context: Context,
    pub stage: Stage,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredId {
    pub doc_id: u32,
    pub id: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClipperSettings {
    pub max_edges_per_cell: usize,
    pub min_short_edge_fraction: f64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Seed {
    pub cell_id: String,
    pub query_level: i32,
    pub member: u32,
    pub contains_center: bool,
    pub edge_indices: Vec<u32>,
    pub member_vertices: usize,
    pub member_zero_vertices: usize,
    pub index_start: u32,
    pub index_end: u32,
    pub first_index_level: Option<i32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Diagnostics {
    pub header: Header,
    pub segment_count: usize,
    pub segment_id: String,
    pub max_doc: u32,
    pub deleted_docs: u32,
    pub stored_ids: Vec<StoredId>,
    pub filter: Filter,
    pub selected_ids: Vec<String>,
    pub sidecar_requested: bool,
    pub sidecar_available: bool,
    pub sidecar_used: bool,
    pub cell_count: u32,
    pub clipper: ClipperSettings,
    pub seeds: Vec<Seed>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Completed {
    pub header: Header,
    pub actual: Vec<String>,
}

pub fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    let temporary = path.with_extension("json.tmp");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    file.write_all(&bytes)?;
    drop(file);
    fs::rename(temporary, path)?;
    Ok(())
}

pub fn progress(root: &Path, context: &Context, stage: Stage) -> Result<()> {
    write_json(
        &root.join("stage.json"),
        &Progress {
            context: context.clone(),
            stage,
        },
    )
}
