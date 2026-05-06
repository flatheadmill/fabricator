// Query a spatial index for geometries that intersect a bounding box or polygon.
//
// Bounding box: --query "-85.28,44.63,-85.08,44.83"
// GeoJSON file: --query leelanau.json

use std::fs;
use std::time::Instant;

use clap::Parser;
use tantivy::collector::Count;
use tantivy::query::{SpatialPredicate, SpatialQuery};
use tantivy::Index;

#[derive(Parser)]
#[command(about = "Find geometries that intersect a bounding box or polygon")]
#[command(allow_negative_numbers = true)]
struct Args {
    /// Path to an existing index directory.
    #[arg(long)]
    dir: String,

    /// Bounding box as "lon_lo,lat_lo,lon_hi,lat_hi" or path to a GeoJSON file.
    #[arg(long)]
    query: String,

    /// Number of iterations.
    #[arg(long, default_value_t = 10)]
    iterations: usize,
}

fn main() -> tantivy::Result<()> {
    let args = Args::parse();

    let index = Index::open_in_dir(&args.dir)?;
    let schema = index.schema();
    let field = schema.get_field("geometry").unwrap();

    let queries = if args.query.contains(',') {
        let parts: Vec<f64> = args.query.split(',').map(|s| s.trim().parse().expect("invalid coordinate")).collect();
        assert_eq!(parts.len(), 4, "bounding box needs 4 values: lon_lo,lat_lo,lon_hi,lat_hi");
        let bounds = [[parts[0], parts[1]], [parts[2], parts[3]]];
        vec![SpatialQuery::intersects_bounds(field, bounds)]
    } else {
        let geojson: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&args.query).expect("could not read file"))
                .expect("invalid JSON");
        let geometry = geojson.get("geometry").unwrap_or(&geojson);
        let gtype = geometry.get("type").and_then(|t| t.as_str()).expect("missing type");
        let coordinates = geometry.get("coordinates").expect("missing coordinates");

        let rings: Vec<Vec<[f64; 2]>> = match gtype {
            "Polygon" => {
                vec![parse_ring(coordinates.as_array().unwrap()[0].as_array().unwrap())]
            }
            "MultiPolygon" => {
                coordinates.as_array().unwrap().iter().map(|poly| {
                    parse_ring(poly.as_array().unwrap()[0].as_array().unwrap())
                }).collect()
            }
            _ => panic!("unsupported geometry type: {}", gtype),
        };

        rings.into_iter().map(|ring| {
            SpatialQuery::with_predicate(field, ring, SpatialPredicate::Intersects)
        }).collect()
    };

    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Warmup.
    let mut hits = 0;
    for q in &queries {
        hits += searcher.search(q, &Count)?;
    }
    eprintln!("{} hits ({} queries)", hits, queries.len());

    let start = Instant::now();
    for _ in 0..args.iterations {
        hits = 0;
        for q in &queries {
            hits += searcher.search(q, &Count)?;
        }
    }
    let elapsed = start.elapsed();
    let per_query = elapsed / args.iterations as u32;

    eprintln!(
        "{} iterations in {:.3}s ({:.3}ms per query)",
        args.iterations,
        elapsed.as_secs_f64(),
        per_query.as_secs_f64() * 1000.0,
    );

    Ok(())
}

fn parse_ring(coords: &[serde_json::Value]) -> Vec<[f64; 2]> {
    coords.iter().map(|pt| {
        let arr = pt.as_array().unwrap();
        [arr[0].as_f64().unwrap(), arr[1].as_f64().unwrap()]
    }).collect()
}
