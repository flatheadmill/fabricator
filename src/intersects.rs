// Query a spatial index for geometries that intersect a bounding box.

use std::time::Instant;

use clap::Parser;
use tantivy::collector::Count;
use tantivy::query::SpatialQuery;
use tantivy::Index;

#[derive(Parser)]
#[command(about = "Find geometries that intersect a bounding box")]
#[command(allow_negative_numbers = true)]
struct Args {
    /// Path to an existing index directory.
    #[arg(long)]
    dir: String,

    /// Longitude low.
    #[arg(long)]
    lon_lo: f64,

    /// Latitude low.
    #[arg(long)]
    lat_lo: f64,

    /// Longitude high.
    #[arg(long)]
    lon_hi: f64,

    /// Latitude high.
    #[arg(long)]
    lat_hi: f64,

    /// Number of iterations.
    #[arg(long, default_value_t = 1)]
    iterations: usize,
}

fn main() -> tantivy::Result<()> {
    let args = Args::parse();

    let bounds = [[args.lon_lo, args.lat_lo], [args.lon_hi, args.lat_hi]];

    let index = Index::open_in_dir(&args.dir)?;
    let schema = index.schema();
    let field = schema.get_field("geometry").unwrap();

    let reader = index.reader()?;
    let searcher = reader.searcher();

    let query = SpatialQuery::intersects_bounds(field, bounds);

    // Warm-up.
    let _ = searcher.search(&query, &Count)?;

    let start = Instant::now();
    let mut hits = 0;
    for _ in 0..args.iterations {
        hits = searcher.search(&query, &Count)?;
    }
    let elapsed = start.elapsed();
    let per_query = elapsed / args.iterations as u32;

    eprintln!("{} hits", hits);
    eprintln!(
        "{} iterations in {:.3}s ({:.3}ms per query)",
        args.iterations,
        elapsed.as_secs_f64(),
        per_query.as_secs_f64() * 1000.0,
    );

    Ok(())
}
