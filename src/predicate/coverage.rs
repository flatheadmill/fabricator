//! Independent exhaustive closed-region coverage in one face's projected UV.
//!
//! This kernel consumes validated simple polygon rings, never engine geometry or
//! covering state. It is the two-dimensional part of the coverage oracle, not a
//! complete predicate harness. Boundary-only and open-member obligations require
//! separate checks. Input and sample margins are measured after projection.

pub type Point = [f64; 2];
pub type Ring = Vec<Point>;
pub type Polygon = Vec<Ring>;
pub type Region = Vec<Polygon>;
type Edge = (Point, Point);

#[derive(Debug, PartialEq)]
pub enum Coverage {
    Covered {
        samples: usize,
    },
    NotCovered {
        witness: Point,
        samples: usize,
    },
    Ambiguous {
        samples: usize,
        unsafe_samples: usize,
    },
}

fn cross(a: Point, b: Point) -> f64 {
    a[0] * b[1] - a[1] * b[0]
}

fn sub(a: Point, b: Point) -> Point {
    [a[0] - b[0], a[1] - b[1]]
}

fn edges(region: &Region) -> Result<Vec<Edge>, &'static str> {
    let mut result = Vec::new();
    for polygon in region {
        if polygon.is_empty() {
            return Err("empty polygon");
        }
        for ring in polygon {
            if ring.len() < 4 || ring.first() != ring.last() {
                return Err("ring must be closed and have at least four positions");
            }
            if ring
                .iter()
                .flatten()
                .any(|v| !v.is_finite() || v.abs() >= 1.0)
            {
                return Err("coordinates must be finite and inside the face");
            }
            for pair in ring.windows(2) {
                if pair[0] == pair[1] {
                    return Err("zero-length edge");
                }
                result.push((pair[0], pair[1]));
            }
        }
    }
    Ok(result)
}

// Only called for samples whose separation from all boundaries was checked.
fn ring_inside(ring: &Ring, point: Point) -> bool {
    let mut inside = false;
    for edge in ring.windows(2) {
        let (a, b) = (edge[0], edge[1]);
        if (a[1] > point[1]) != (b[1] > point[1]) {
            let x = a[0] + (point[1] - a[1]) * (b[0] - a[0]) / (b[1] - a[1]);
            if x > point[0] {
                inside = !inside;
            }
        }
    }
    inside
}

pub(crate) fn region_inside(region: &Region, point: Point) -> bool {
    region.iter().any(|polygon| {
        polygon
            .iter()
            .fold(false, |inside, ring| inside ^ ring_inside(ring, point))
    })
}

pub(crate) fn distance_to_edge(point: Point, (a, b): Edge) -> f64 {
    let delta = sub(b, a);
    let offset = sub(point, a);
    let length2 = delta[0] * delta[0] + delta[1] * delta[1];
    let t = ((offset[0] * delta[0] + offset[1] * delta[1]) / length2).clamp(0.0, 1.0);
    (offset[0] - t * delta[0]).hypot(offset[1] - t * delta[1])
}

fn intersection_x((a, b): Edge, (c, d): Edge) -> Option<f64> {
    let ab = sub(b, a);
    let cd = sub(d, c);
    let denominator = cross(ab, cd);
    if denominator == 0.0 {
        return None;
    }
    let offset = sub(c, a);
    let t = cross(offset, cd) / denominator;
    let u = cross(offset, ab) / denominator;
    if t > 0.0 && t < 1.0 && u > 0.0 && u < 1.0 {
        Some(a[0] + t * ab[0])
    } else {
        None
    }
}

// This metric guard includes pairs across members and across subject/container.
// Only a shared endpoint within one ring is exempted. Simplicity and valid
// GeoJSON hole ownership remain obligations of the full input validator.
fn clear_input(container: &Region, subject: &Region, margin: f64) -> bool {
    let mut tagged = Vec::new();
    for (ring_id, ring) in container.iter().chain(subject).flatten().enumerate() {
        for pair in ring.windows(2) {
            tagged.push((ring_id, (pair[0], pair[1])));
        }
    }
    for (i, &(owner_a, ab)) in tagged.iter().enumerate() {
        for &(owner_b, cd) in &tagged[i + 1..] {
            for (point, edge) in [(ab.0, cd), (ab.1, cd), (cd.0, ab), (cd.1, ab)] {
                if owner_a == owner_b && (point == edge.0 || point == edge.1) {
                    continue;
                }
                if distance_to_edge(point, edge) <= margin {
                    return false;
                }
            }
            if intersection_x(ab, cd).is_some() {
                for (point, (a, b)) in [(ab.0, cd), (ab.1, cd), (cd.0, ab), (cd.1, ab)] {
                    let edge = sub(b, a);
                    let line_distance = cross(edge, sub(point, a)).abs() / edge[0].hypot(edge[1]);
                    if line_distance <= margin {
                        return false;
                    }
                }
            }
        }
    }
    true
}

/// Decide whether any two-dimensional subject interior is outside the container.
///
/// Caller must validate simplicity and GeoJSON hole ownership (no nested holes).
/// This function rejects malformed rings and inadequate cross-geometry input
/// clearance, and marks inadequate sample clearance as ambiguous. It uses
/// ordinary f64 arrangement arithmetic, so it is
/// not an exact-arithmetic certificate for adversarial near-degenerate inputs.
pub fn covers(container: &Region, subject: &Region, margin: f64) -> Result<Coverage, &'static str> {
    if !margin.is_finite() || margin <= 0.0 {
        return Err("positive finite sample margin required");
    }
    let mut all_edges = edges(container)?;
    all_edges.extend(edges(subject)?);
    if !clear_input(container, subject, margin) {
        return Err("input boundary incidence or inadequate projected clearance");
    }
    let mut events: Vec<f64> = all_edges.iter().flat_map(|(a, b)| [a[0], b[0]]).collect();
    for (i, &a) in all_edges.iter().enumerate() {
        for &b in &all_edges[i + 1..] {
            if let Some(x) = intersection_x(a, b) {
                events.push(x);
            }
        }
    }
    events.sort_by(f64::total_cmp);
    events.dedup();
    let mut samples = 0;
    let mut unsafe_samples = 0;
    for slab in events.windows(2) {
        let x = slab[0] + (slab[1] - slab[0]) / 2.0;
        if !(slab[0] < x && x < slab[1]) {
            unsafe_samples += 1;
            continue;
        }
        let mut ys = Vec::new();
        for &(a, b) in &all_edges {
            if a[0].min(b[0]) < x && x < a[0].max(b[0]) {
                ys.push(a[1] + (x - a[0]) * (b[1] - a[1]) / (b[0] - a[0]));
            }
        }
        ys.sort_by(f64::total_cmp);
        // Equal y values inside an open slab indicate incidence or an event
        // missed by finite arithmetic. Keep the zero interval as unsafe evidence.
        for interval in ys.windows(2) {
            let y = interval[0] + (interval[1] - interval[0]) / 2.0;
            let point = [x, y];
            samples += 1;
            if !(interval[0] < y && y < interval[1])
                || all_edges
                    .iter()
                    .any(|&edge| distance_to_edge(point, edge) <= margin)
            {
                unsafe_samples += 1;
                continue;
            }
            if region_inside(subject, point) && !region_inside(container, point) {
                return Ok(Coverage::NotCovered {
                    witness: point,
                    samples,
                });
            }
        }
    }
    // A clear outside witness suffices even if another cell is ambiguous. A
    // positive coverage result requires that every arrangement sample was safe.
    if unsafe_samples > 0 {
        Ok(Coverage::Ambiguous {
            samples,
            unsafe_samples,
        })
    } else {
        Ok(Coverage::Covered { samples })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rectangle(x0: f64, y0: f64, x1: f64, y1: f64) -> Ring {
        vec![[x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0]]
    }

    fn covered(container: &Region, subject: &Region) -> bool {
        match covers(container, subject, 1e-10).unwrap() {
            Coverage::Covered { .. } => true,
            Coverage::NotCovered { .. } => false,
            result => panic!("hand fixture must be unambiguous: {result:?}"),
        }
    }

    #[test]
    fn union_can_cover_without_a_single_containing_member() {
        let left = vec![rectangle(-0.75, -0.5, 0.25, 0.5)];
        let right = vec![rectangle(-0.25, -0.6, 0.75, 0.6)];
        let subject = vec![vec![rectangle(-0.6, -0.3, 0.6, 0.3)]];
        assert!(!covered(&vec![left.clone()], &subject));
        assert!(!covered(&vec![right.clone()], &subject));
        assert!(covered(&vec![left, right], &subject));
    }

    #[test]
    fn a_hole_inside_the_subject_disproves_coverage_without_boundary_crossings() {
        let container = vec![vec![
            rectangle(-0.8, -0.8, 0.8, 0.8),
            rectangle(-0.1, -0.1, 0.1, 0.1),
        ]];
        let subject = vec![vec![rectangle(-0.4, -0.4, 0.4, 0.4)]];
        assert!(!covered(&container, &subject));
        let safe = vec![vec![rectangle(0.2, 0.2, 0.4, 0.4)]];
        assert!(covered(&container, &safe));
    }

    #[test]
    fn internal_member_boundaries_do_not_exclude_coverage() {
        let container = vec![
            vec![rectangle(-0.8, -0.8, 0.8, 0.8)],
            vec![rectangle(-0.2, -0.2, 0.2, 0.2)],
        ];
        assert!(covered(
            &container,
            &vec![vec![rectangle(0.1, 0.1, 0.4, 0.4)]]
        ));
    }

    #[test]
    fn concave_notch_is_outside_even_with_no_boundary_crossing() {
        let u = vec![
            [-0.8, -0.8],
            [0.8, -0.8],
            [0.8, 0.8],
            [0.3, 0.8],
            [0.3, -0.3],
            [-0.3, -0.3],
            [-0.3, 0.8],
            [-0.8, 0.8],
            [-0.8, -0.8],
        ];
        let container = vec![vec![u]];
        assert!(!covered(
            &container,
            &vec![vec![rectangle(-0.2, 0.0, 0.2, 0.5)]]
        ));
        assert!(covered(
            &container,
            &vec![vec![rectangle(-0.2, -0.7, 0.2, -0.4)]]
        ));
    }

    #[test]
    fn winding_and_member_order_preserve_region() {
        let mut container = vec![
            vec![
                rectangle(-0.8, -0.8, 0.8, 0.8),
                rectangle(-0.2, -0.2, 0.2, 0.2),
            ],
            vec![rectangle(-0.1, -0.1, 0.1, 0.1)],
        ];
        let subject = vec![vec![rectangle(-0.05, -0.05, 0.05, 0.05)]];
        assert!(covered(&container, &subject));
        for polygon in &mut container {
            for ring in polygon {
                ring.reverse();
            }
        }
        container.reverse();
        assert!(covered(&container, &subject));
    }

    #[test]
    fn thin_unresolved_face_is_not_reported_as_agreement() {
        let container = vec![vec![rectangle(-0.8, -0.8, 0.8, 0.8)]];
        let subject = vec![vec![rectangle(-0.4, -0.4, 0.4, 0.4)]];
        assert!(matches!(
            covers(&container, &subject, 0.25).unwrap(),
            Coverage::Ambiguous { .. }
        ));
    }

    #[test]
    fn coincident_subject_and_container_are_not_ordinary_coverage() {
        let region = vec![vec![rectangle(-0.2, -0.2, 0.2, 0.2)]];
        assert_eq!(
            covers(&region, &region, 1e-10),
            Err("input boundary incidence or inadequate projected clearance")
        );
    }

    #[test]
    fn diagonal_members_jointly_cover_a_rectangle() {
        // Parallel sloping boundaries overlap across the middle of the square.
        // Neither triangle alone contains the subject rectangle.
        let lower = vec![vec![
            [-0.85, -0.8],
            [0.75, -0.8],
            [-0.85, 0.95],
            [-0.85, -0.8],
        ]];
        let upper = vec![vec![[-0.75, 0.8], [0.85, -0.95], [0.85, 0.8], [-0.75, 0.8]]];
        let subject = vec![vec![rectangle(-0.4, -0.4, 0.4, 0.4)]];
        assert!(!covered(&vec![lower.clone()], &subject));
        assert!(!covered(&vec![upper.clone()], &subject));
        assert!(covered(&vec![lower, upper], &subject));
    }

    #[test]
    fn diagonal_hole_and_disconnected_subjects_are_all_examined() {
        let hole = vec![
            [0.0, -0.2],
            [0.2, 0.0],
            [0.0, 0.2],
            [-0.2, 0.0],
            [0.0, -0.2],
        ];
        let container = vec![vec![rectangle(-0.8, -0.8, 0.8, 0.8), hole]];
        let safe = vec![rectangle(0.4, 0.4, 0.6, 0.6)];
        let unsafe_member = vec![rectangle(-0.05, -0.05, 0.05, 0.05)];
        assert!(covered(&container, &vec![safe.clone()]));
        assert!(!covered(&container, &vec![safe, unsafe_member]));
    }
}
