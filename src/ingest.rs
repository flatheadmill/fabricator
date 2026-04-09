// Ingest newline-delimited GeoJSON into a tantivy index. Each line is a GeoJSON
// Feature with properties and geometry. No merging is performed; use the merge
// tool to consolidate segments afterward.

use std::fs::File;
use std::io::{BufRead, BufReader};

use clap::Parser;
use tantivy::indexer::NoMergePolicy;
use tantivy::schema::{Schema, SPATIAL, STORED, STRING};
use tantivy::{Index, IndexWriter, TantivyDocument};

#[derive(Parser)]
#[command(about = "Ingest newline-delimited GeoJSON into a tantivy spatial index")]
struct Args {
    /// Path to the newline-delimited GeoJSON file.
    #[arg(long)]
    input: String,

    /// Directory to write the index. Created if it does not exist.
    #[arg(long)]
    dir: String,

    /// Documents per segment.
    #[arg(long, default_value_t = 100_000)]
    batch_size: usize,

    /// Maximum number of documents to ingest. Omit for no limit.
    #[arg(long)]
    limit: Option<usize>,

    /// Writer memory budget in gigabytes.
    #[arg(long, default_value_t = 1)]
    memory_gb: usize,
}

fn main() -> tantivy::Result<()> {
    let args = Args::parse();

    let mut schema_builder = Schema::builder();
    schema_builder.add_text_field("name", STRING | STORED);
    schema_builder.add_spatial_field("geometry", SPATIAL);
    let schema = schema_builder.build();

    std::fs::create_dir_all(&args.dir).expect("could not create index directory");

    let index = if Index::open_in_dir(&args.dir).is_ok() {
        Index::open_in_dir(&args.dir)?
    } else {
        Index::create_in_dir(&args.dir, schema.clone())?
    };

    let memory_bytes = args.memory_gb * 1_073_741_824;
    let mut writer: IndexWriter = index.writer(memory_bytes)?;
    writer.set_merge_policy(Box::new(NoMergePolicy));

    let file = File::open(&args.input).expect("could not open input file");
    let reader = BufReader::new(file);

    let mut count = 0usize;
    let mut batch = 0usize;
    let mut skipped = 0usize;

    for line in reader.lines() {
        let line = line.expect("could not read line");
        if line.trim().is_empty() {
            continue;
        }
        let feature: serde_json::Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };
        let obj = match feature.as_object() {
            Some(o) => o,
            None => {
                skipped += 1;
                continue;
            }
        };
        let name = obj
            .get("properties")
            .and_then(|p| p.get("name"))
            .and_then(|n| n.as_str())
            .unwrap_or("");
        let geometry = match obj.get("geometry") {
            Some(g) => g,
            None => {
                skipped += 1;
                continue;
            }
        };
        let doc_json = format!(
            r#"{{"name":{},"geometry":{}}}"#,
            serde_json::Value::String(name.to_string()),
            geometry,
        );
        let doc = match TantivyDocument::parse_json(&schema, &doc_json) {
            Ok(d) => d,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };
        writer.add_document(doc)?;
        count += 1;

        if count % args.batch_size == 0 {
            writer.commit()?;
            batch += 1;
            eprintln!("committed batch {} ({} docs)", batch, count);
        }
        if let Some(limit) = args.limit {
            if count >= limit {
                break;
            }
        }
    }
    if count % args.batch_size != 0 {
        writer.commit()?;
        batch += 1;
        eprintln!("committed batch {} ({} docs)", batch, count);
    }

    writer.wait_merging_threads()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();
    eprintln!(
        "{} segments, {} docs ingested, {} skipped",
        searcher.segment_readers().len(),
        count,
        skipped,
    );

    Ok(())
}
