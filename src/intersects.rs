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

    let start = Instant::now();
    let hits = searcher.search(&query, &Count)?;
    let elapsed = start.elapsed();

    eprintln!("{} hits in {:.3}ms", hits, elapsed.as_secs_f64() * 1000.0);

    Ok(())
}
