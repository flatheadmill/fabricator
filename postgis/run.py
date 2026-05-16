#!/usr/bin/env python3

import subprocess
import sys

def run_query(name, sql):
    result = subprocess.run(
        ["docker", "exec", "-i", "postgis", "psql", "-U", "postgres", "-d", "michigan"],
        input=sql,
        capture_output=True,
        text=True,
    )
    output = result.stdout + result.stderr
    for line in output.splitlines():
        if "NOTICE" in line or "Time:" in line:
            print(f"  {line.strip()}")

def main():
    query = sys.argv[1] if len(sys.argv) > 1 else "all"

    queries = {}

    queries["kalkaska_intersects_geometry"] = """
        SELECT COUNT(*) FROM michigan
        WHERE ST_Intersects(wkb_geometry, ST_MakeEnvelope(-85.28, 44.63, -85.08, 44.83, 4326))
    """

    queries["kalkaska_intersects_geography"] = """
        SELECT COUNT(*) FROM michigan
        WHERE ST_Intersects(geog, ST_MakeEnvelope(-85.28, 44.63, -85.08, 44.83, 4326)::geography)
    """

    queries["kalkaska_contains_geometry"] = """
        SELECT COUNT(*) FROM michigan
        WHERE ST_Contains(wkb_geometry, ST_MakeEnvelope(-85.28, 44.63, -85.08, 44.83, 4326))
    """

    queries["kalkaska_contains_geography"] = """
        SELECT COUNT(*) FROM michigan
        WHERE ST_Contains(geog::geometry, ST_MakeEnvelope(-85.28, 44.63, -85.08, 44.83, 4326))
    """

    queries["kalkaska_within_geometry"] = """
        SELECT COUNT(*) FROM michigan
        WHERE ST_Within(wkb_geometry, ST_MakeEnvelope(-85.28, 44.63, -85.08, 44.83, 4326))
    """

    queries["kalkaska_within_geography"] = """
        SELECT COUNT(*) FROM michigan
        WHERE ST_Within(geog::geometry, ST_MakeEnvelope(-85.28, 44.63, -85.08, 44.83, 4326))
    """

    import json
    import os

    leelanau_path = os.path.join(os.path.dirname(__file__), "..", "leelanau_peninsula.json")
    if os.path.exists(leelanau_path):
        with open(leelanau_path) as f:
            geojson = json.load(f)
        geom = geojson.get("geometry", geojson)
        coords = geom["coordinates"][0]
        points = ", ".join(f"{c[0]} {c[1]}" for c in coords)

        queries["leelanau_intersects_geometry"] = f"""
            SELECT COUNT(*) FROM michigan
            WHERE ST_Intersects(wkb_geometry, ST_GeomFromText('POLYGON(({points}))', 4326))
        """

        queries["leelanau_intersects_geography"] = f"""
            SELECT COUNT(*) FROM michigan
            WHERE ST_Intersects(geog, ST_GeomFromText('POLYGON(({points}))', 4326)::geography)
        """

    to_run = {}
    if query == "all":
        to_run = queries
    else:
        matching = {k: v for k, v in queries.items() if query in k}
        if not matching:
            print(f"no query matching '{query}'")
            print(f"available: {', '.join(sorted(queries.keys()))}")
            sys.exit(1)
        to_run = matching

    iterations = 10
    for name, inner_sql in sorted(to_run.items()):
        sql = f"""SET max_parallel_workers_per_gather = 0;
\\timing on
DO $body$
DECLARE i INT; cnt BIGINT;
BEGIN
  FOR i IN 1..{iterations} LOOP
    {inner_sql.strip()} INTO cnt;
  END LOOP;
  RAISE NOTICE 'count: %', cnt;
END
$body$;
"""
        print(f"{name}:")
        run_query(name, sql)
        print()


if __name__ == "__main__":
    main()
