SET max_parallel_workers_per_gather = 0;
\timing on
DO $body$
DECLARE i INT; cnt BIGINT;
BEGIN
  FOR i IN 1..10 LOOP
    SELECT COUNT(*) INTO cnt FROM michigan
    WHERE ST_Contains(geog::geometry, ST_MakeEnvelope(-85.28, 44.63, -85.08, 44.83, 4326));
  END LOOP;
  RAISE NOTICE 'count: %', cnt;
END
$body$;
