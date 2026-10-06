# Intersects cases

These files record raw longitude and latitude coordinates for direct Intersects
replay. Plane and Sphere have separate files. Document, member and ring order
are part of each input. The reference uses projected face-zero UV coordinates
with a margin of 1e-9.

The crossing cases place a vertical indexed rectangle across the second of two
query polygons. No vertex lies inside the other polygon, so four proper edge
crossings are the only witnesses. The containment cases place an indexed point
inside the second query polygon and outside the first. A remote indexed polygon
is disjoint from both query members.

The constructed unequal-member cases replace the first query polygon with a
triangle. Its four positions include closure; the second member has five. The
indexed rectangle still crosses only the second member. These are constructed
cases, not copies of a recorded failing index.

Each expected set comes from the geometric construction and is checked by the
raw reference tests. These tests do not establish an engine result or a traversal
mechanism. Replay calibration must check the workers after normal ingestion.

A case requires version, name, surface, documents, query, filter, sidecar, margin
and expected. Each document has a unique nonempty id and a raw GeoJSON geometry.
The expected object records oracle_version and sorted unique ids. Filter mode is
none, all, include or exclude; include and exclude carry ids. Empty include
selects nothing. Unknown fields and unknown or repeated IDs are rejected.

The current reference judges Points, single-ring Polygons and MultiPolygons
whose members have one ring. It checks raw cardinality and coordinates before
rejecting unsupported families or holes. Malformed geometry, unsupported scope
and insufficient numerical clearance are distinct outcomes. Every document is
judged before filtering, including documents excluded from the semantic result.
The reference uses the engine's projection and proper-crossing arithmetic, but
no engine geometry sets, coverings or indexed containment state.

The normal-ingestion cases use sidecar=false. The fresh-segment writer does not
populate the cell doc-ID sidecar. A worker asked to use a missing sidecar fails
before the query; these cases do not claim to exercise that reader capability.

Checks proceed through case metadata and IDs, raw structure and coordinates,
margin and size limits, supported families, projected ring validity and
clearance, and document/query boundary incidence before computing witnesses.
The first rejection determines the outcome; a filter never suppresses it.

Run the reference and case tests with `cargo test --test predicate`.
