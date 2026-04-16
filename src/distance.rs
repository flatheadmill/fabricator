// Query a spatial index for geometries within a distance of a bounding box.
// The distance is edge-to-edge: from the nearest edge of the bounding box to
// the nearest edge of each candidate geometry.

use std::time::Instant;

use clap::Parser;
use tantivy::spatial::cell_index_reader::CellIndexReader;
use tantivy::spatial::closest_edge_query::ClosestEdgeQuery;
use tantivy::spatial::edge_cache::EdgeCache;
use tantivy::spatial::edge_reader::EdgeReader;
use tantivy::spatial::geometry::Geometry;
use tantivy::spatial::geometry_set::to_geometry_set;
use tantivy::spatial::plane::Plane;
use tantivy::spatial::s1chord_angle::S1ChordAngle;
use tantivy::spatial::sphere::Sphere;
use tantivy::Index;

const EARTH_RADIUS_METERS: f64 = 6_371_000.0;

#[derive(Parser)]
#[command(about = "Find geometries within a distance of a bounding box")]
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

    /// Maximum distance in meters (edge-to-edge).
    #[arg(long)]
    radius: f64,
}

fn main() -> tantivy::Result<()> {
    let args = Args::parse();

    let ring = vec![
        [args.lon_lo, args.lat_lo],
        [args.lon_hi, args.lat_lo],
        [args.lon_hi, args.lat_hi],
        [args.lon_lo, args.lat_hi],
        [args.lon_lo, args.lat_lo],
    ];
    let plane_geometry = Geometry::<Plane>::Polygon(vec![ring]);
    let projected = plane_geometry.project::<Sphere>();
    let set = to_geometry_set(&projected, 0);

    let radius_radians = args.radius / EARTH_RADIUS_METERS;
    let max_distance = S1ChordAngle::from_radians(radius_radians);
    let query = ClosestEdgeQuery::within(set, max_distance);

    let index = Index::open_in_dir(&args.dir)?;
    let schema = index.schema();
    let field = schema.get_field("geometry").unwrap();

    let reader = index.reader()?;
    let searcher = reader.searcher();

    let start = Instant::now();
    let mut total_hits = 0usize;

    for seg in searcher.segment_readers() {
        let spatial = seg.spatial_fields().get_field(field)?;
        if let Some(spatial_reader) = spatial {
            let cell_reader = CellIndexReader::open(spatial_reader.cells_bytes());
            let edge_reader = EdgeReader::<Sphere>::open(spatial_reader.edges_bytes());
            let mut edge_cache = EdgeCache::new(vec![edge_reader], 100_000);

            let results = query.search_segment(&cell_reader, &mut edge_cache);
            total_hits += results.len();
        }
    }

    let elapsed = start.elapsed();
    eprintln!("{} hits in {:.3}ms", total_hits, elapsed.as_secs_f64() * 1000.0);

    Ok(())
}
