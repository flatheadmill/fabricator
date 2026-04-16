// Dump the cell index of all segments as JSONL. One JSON object per cell, one file for the index.

use std::io::Write;

use clap::Parser;
use tantivy::spatial::cell_index_reader::CellIndexReader;
use tantivy::spatial::edge_cache::EdgeCache;
use tantivy::spatial::edge_reader::EdgeReader;
use tantivy::spatial::sphere::Sphere;
use tantivy::Index;

#[derive(Parser)]
#[command(about = "Dump segment cell indexes as JSONL")]
struct Args {
    /// Path to an existing index directory.
    #[arg(long)]
    dir: String,
}

fn main() -> tantivy::Result<()> {
    let args = Args::parse();

    let index = Index::open_in_dir(&args.dir)?;
    let schema = index.schema();
    let field = schema.get_field("geometry").unwrap();

    let reader = index.reader()?;
    let searcher = reader.searcher();

    let filename = format!("{}/cells.jsonl", args.dir);
    let mut out =
        std::io::BufWriter::new(std::fs::File::create(&filename).expect("could not create output file"));

    let mut total_cells = 0u32;

    for (seg_idx, seg) in searcher.segment_readers().iter().enumerate() {
        let spatial = seg.spatial_fields().get_field(field)?;
        let spatial_reader = match spatial {
            Some(r) => r,
            None => continue,
        };

        let cell_reader = CellIndexReader::open(spatial_reader.cells_bytes());
        let edge_reader = EdgeReader::<Sphere>::open(spatial_reader.edges_bytes());
        let geometry_count = edge_reader.geometry_count();
        let edge_cache = EdgeCache::new(vec![edge_reader], 100_000);
        let segment_id = seg.segment_id().uuid_string();

        let mut cell_count = 0u32;
        for cell in cell_reader.iter() {
            let total_edges: usize = cell.shapes.iter().map(|s| s.edge_indices.len()).sum();
            let contains_count = cell.shapes.iter().filter(|s| s.contains_center).count();
            let closed_count = if cell.cell_id.level() <= 5 {
                cell.shapes
                    .iter()
                    .filter(|s| edge_cache.get(s.geometry_id).edge_set().closed)
                    .count()
            } else {
                0
            };
            let center = cell.cell_id.to_point();
            let lat = center[2].asin().to_degrees();
            let lon = center[1].atan2(center[0]).to_degrees();
            write!(
                out,
                "{{\"segment\":\"{}\",\"cell_id\":{},\"level\":{},\"face\":{},\"lat\":{:.4},\"lon\":{:.4},\"shapes\":{},\"contains\":{},\"closed\":{},\"edges\":{}",
                segment_id,
                cell.cell_id.0,
                cell.cell_id.level(),
                cell.cell_id.face(),
                lat,
                lon,
                cell.shapes.len(),
                contains_count,
                closed_count,
                total_edges,
            )
            .unwrap();

            // For coarse cells (level <= 5), include per-shape detail.
            if cell.cell_id.level() <= 5 {
                write!(out, ",\"detail\":[").unwrap();
                for (i, shape) in cell.shapes.iter().enumerate() {
                    if i > 0 {
                        write!(out, ",").unwrap();
                    }
                    write!(
                        out,
                        "{{\"geometry_id\":{},\"contains_center\":{},\"edges\":{}}}",
                        shape.geometry_id.1,
                        shape.contains_center,
                        shape.edge_indices.len(),
                    )
                    .unwrap();
                }
                write!(out, "]").unwrap();
            }

            writeln!(out, "}}").unwrap();
            cell_count += 1;
        }

        eprintln!(
            "segment {} ({}): {} cells, {} geometries",
            seg_idx, segment_id, cell_count, geometry_count,
        );
        total_cells += cell_count;
    }

    eprintln!("{} total cells -> {}", total_cells, filename);

    Ok(())
}
