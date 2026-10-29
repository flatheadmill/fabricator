# Plane relation corpus cases

`michigan_contacts.json` retains the complete Lower Peninsula query and three
GeoFabrik geometries that exposed boundary-relation behavior.
They are corpus characterization, not Tantivy unit fixtures:

- 5775305 intersects the query and is not covered by it.
- 2079965 intersects the query and is not covered by it.
- 1469871 intersects the query and is covered by it.

The Tantivy repository carries compact synthetic controls for the underlying
contact and coverage mechanisms. Fabricator owns replay of these full source
geometries and the larger Michigan corpus.
