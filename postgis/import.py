#!/usr/bin/env python3

import json
import subprocess
import sys

def main():
    input_path = sys.argv[1] if len(sys.argv) > 1 else "/Users/alan/code/geofabrik/michigan.json"
    db = sys.argv[2] if len(sys.argv) > 2 else "michigan"

    def psql(sql):
        proc = subprocess.Popen(
            ["docker", "exec", "-i", "postgis", "psql", "-U", "postgres", "-d", db],
            stdin=subprocess.PIPE,
            text=True,
        )
        proc.communicate(sql)
        return proc.returncode

    psql("""
        DROP TABLE IF EXISTS michigan CASCADE;
        CREATE TABLE michigan (
            id SERIAL PRIMARY KEY,
            name TEXT,
            geom_type TEXT,
            properties JSONB,
            wkb_geometry geometry(Geometry, 4326),
            geog geography
        );
    """)

    count = 0
    skipped = 0
    batch = []
    batch_size = 10000

    def flush(batch):
        if not batch:
            return
        sql = "BEGIN;\n" + "\n".join(batch) + "\nCOMMIT;\n"
        psql(sql)

    with open(input_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                feature = json.loads(line)
            except json.JSONDecodeError:
                skipped += 1
                continue

            properties = feature.get("properties", {})
            geometry = feature.get("geometry")
            if geometry is None:
                skipped += 1
                continue

            name = properties.get("name", "") or ""
            geom_type = geometry.get("type", "")
            props_json = json.dumps(properties).replace("'", "''")
            geom_json = json.dumps(geometry).replace("'", "''")
            name_escaped = name.replace("'", "''")

            stmt = (
                f"INSERT INTO michigan (name, geom_type, properties, wkb_geometry) "
                f"VALUES ('{name_escaped}', '{geom_type}', '{props_json}'::jsonb, "
                f"ST_SetSRID(ST_GeomFromGeoJSON('{geom_json}'), 4326));"
            )
            batch.append(stmt)

            count += 1
            if count % batch_size == 0:
                flush(batch)
                batch = []
            if count % 10000 == 0:
                print(f"imported {count} features ({skipped} skipped)", file=sys.stderr)

    flush(batch)
    print(f"done: {count} imported, {skipped} skipped", file=sys.stderr)

    print("updating geography column...", file=sys.stderr)
    psql("UPDATE michigan SET geog = wkb_geometry::geography;")

    print("creating indexes...", file=sys.stderr)
    psql("""
        CREATE INDEX michigan_geom_idx ON michigan USING GIST (wkb_geometry);
        CREATE INDEX michigan_geog_idx ON michigan USING GIST (geog);
        CREATE INDEX michigan_props_idx ON michigan USING GIN (properties);
        CREATE INDEX michigan_name_idx ON michigan USING btree (name);
        CREATE INDEX michigan_geom_type_idx ON michigan USING btree (geom_type);
    """)
    print("indexes created", file=sys.stderr)


if __name__ == "__main__":
    main()
