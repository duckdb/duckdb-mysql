.timer on

ATTACH 'host=localhost user=root port=0 database=mysqlscanner' AS loader (TYPE MYSQL_SCANNER);
DETACH loader;
ATTACH 'host=localhost user=root port=0 database=mysqlscanner' AS s1 (TYPE MYSQL_SCANNER);

DROP TABLE IF EXISTS s1.bench_speed;

CALL mysql_execute('s1', '
CREATE TABLE bench_speed (
    id INTEGER NOT NULL PRIMARY KEY,
    indexed_col INTEGER NOT NULL,
    status VARCHAR(20) NOT NULL,
    payload VARCHAR(500) NOT NULL,
    INDEX idx_bench_indexed (indexed_col),
    INDEX idx_bench_status (status(20))
)');

CALL mysql_clear_cache();

INSERT INTO s1.bench_speed
SELECT
    range AS id,
    range % 100 AS indexed_col,
    CASE WHEN range % 3 = 0 THEN 'active'
         WHEN range % 3 = 1 THEN 'pending'
         ELSE 'closed' END,
    REPEAT('x', 200)
FROM range(50000);

CALL mysql_execute('s1', 'ANALYZE TABLE bench_speed');

SELECT '=== BENCHMARK 1: PK Point Lookup (1/50000 rows) ===' AS benchmark;

SELECT '--- Federation ON ---' AS mode;
SET GLOBAL mysql_experimental_filter_pushdown=true;
CALL mysql_clear_cache();

SELECT id, indexed_col FROM s1.bench_speed WHERE id = 25000;
SELECT id, indexed_col FROM s1.bench_speed WHERE id = 25000;
SELECT id, indexed_col FROM s1.bench_speed WHERE id = 25000;

SELECT '--- Federation OFF ---' AS mode;
SET GLOBAL mysql_experimental_filter_pushdown=false;
CALL mysql_clear_cache();

SELECT id, indexed_col FROM s1.bench_speed WHERE id = 25000;
SELECT id, indexed_col FROM s1.bench_speed WHERE id = 25000;
SELECT id, indexed_col FROM s1.bench_speed WHERE id = 25000;

SELECT '=== BENCHMARK 2: Indexed Equality (500/50000 rows, 1%) ===' AS benchmark;

SELECT '--- Federation ON ---' AS mode;
SET GLOBAL mysql_experimental_filter_pushdown=true;
CALL mysql_clear_cache();

SELECT id, indexed_col FROM s1.bench_speed WHERE indexed_col = 42;
SELECT id, indexed_col FROM s1.bench_speed WHERE indexed_col = 42;
SELECT id, indexed_col FROM s1.bench_speed WHERE indexed_col = 42;

SELECT '--- Federation OFF ---' AS mode;
SET GLOBAL mysql_experimental_filter_pushdown=false;
CALL mysql_clear_cache();

SELECT id, indexed_col FROM s1.bench_speed WHERE indexed_col = 42;
SELECT id, indexed_col FROM s1.bench_speed WHERE indexed_col = 42;
SELECT id, indexed_col FROM s1.bench_speed WHERE indexed_col = 42;

SELECT '=== BENCHMARK 3: Compound Filter (~170/50000 rows, 0.33%) ===' AS benchmark;

SELECT '--- Federation ON ---' AS mode;
SET GLOBAL mysql_experimental_filter_pushdown=true;
CALL mysql_clear_cache();

SELECT id, indexed_col FROM s1.bench_speed WHERE indexed_col = 10 AND status = 'active';
SELECT id, indexed_col FROM s1.bench_speed WHERE indexed_col = 10 AND status = 'active';
SELECT id, indexed_col FROM s1.bench_speed WHERE indexed_col = 10 AND status = 'active';

SELECT '--- Federation OFF ---' AS mode;
SET GLOBAL mysql_experimental_filter_pushdown=false;
CALL mysql_clear_cache();

SELECT id, indexed_col FROM s1.bench_speed WHERE indexed_col = 10 AND status = 'active';
SELECT id, indexed_col FROM s1.bench_speed WHERE indexed_col = 10 AND status = 'active';
SELECT id, indexed_col FROM s1.bench_speed WHERE indexed_col = 10 AND status = 'active';

SELECT '=== BENCHMARK 4: COUNT(*) Full Table (50000 rows) ===' AS benchmark;

SELECT '--- Federation ON ---' AS mode;
SET GLOBAL mysql_experimental_filter_pushdown=true;
CALL mysql_clear_cache();

SELECT COUNT(*) FROM s1.bench_speed;
SELECT COUNT(*) FROM s1.bench_speed;
SELECT COUNT(*) FROM s1.bench_speed;

SELECT '--- Federation OFF ---' AS mode;
SET GLOBAL mysql_experimental_filter_pushdown=false;
CALL mysql_clear_cache();

SELECT COUNT(*) FROM s1.bench_speed;
SELECT COUNT(*) FROM s1.bench_speed;
SELECT COUNT(*) FROM s1.bench_speed;

SELECT '=== BENCHMARK 5: COUNT(*) with WHERE (500/50000 rows) ===' AS benchmark;

SELECT '--- Federation ON ---' AS mode;
SET GLOBAL mysql_experimental_filter_pushdown=true;
CALL mysql_clear_cache();

SELECT COUNT(*) FROM s1.bench_speed WHERE indexed_col = 77;
SELECT COUNT(*) FROM s1.bench_speed WHERE indexed_col = 77;
SELECT COUNT(*) FROM s1.bench_speed WHERE indexed_col = 77;

SELECT '--- Federation OFF ---' AS mode;
SET GLOBAL mysql_experimental_filter_pushdown=false;
CALL mysql_clear_cache();

SELECT COUNT(*) FROM s1.bench_speed WHERE indexed_col = 77;
SELECT COUNT(*) FROM s1.bench_speed WHERE indexed_col = 77;
SELECT COUNT(*) FROM s1.bench_speed WHERE indexed_col = 77;

SELECT '=== BENCHMARK 6: ORDER BY + LIMIT 5 (out of 50K rows) ===' AS benchmark;

SELECT '--- Federation ON ---' AS mode;
SET GLOBAL mysql_experimental_filter_pushdown=true;
CALL mysql_clear_cache();

SELECT id, indexed_col FROM s1.bench_speed ORDER BY indexed_col, id LIMIT 5;
SELECT id, indexed_col FROM s1.bench_speed ORDER BY indexed_col, id LIMIT 5;
SELECT id, indexed_col FROM s1.bench_speed ORDER BY indexed_col, id LIMIT 5;

SELECT '--- Federation OFF ---' AS mode;
SET GLOBAL mysql_experimental_filter_pushdown=false;
CALL mysql_clear_cache();

SELECT id, indexed_col FROM s1.bench_speed ORDER BY indexed_col, id LIMIT 5;
SELECT id, indexed_col FROM s1.bench_speed ORDER BY indexed_col, id LIMIT 5;
SELECT id, indexed_col FROM s1.bench_speed ORDER BY indexed_col, id LIMIT 5;

SELECT '=== BENCHMARK 7: SUM(id) with WHERE (500/50000 rows) ===' AS benchmark;

SELECT '--- Federation ON ---' AS mode;
SET GLOBAL mysql_experimental_filter_pushdown=true;
CALL mysql_clear_cache();

SELECT SUM(id) FROM s1.bench_speed WHERE indexed_col = 0;
SELECT SUM(id) FROM s1.bench_speed WHERE indexed_col = 0;
SELECT SUM(id) FROM s1.bench_speed WHERE indexed_col = 0;

SELECT '--- Federation OFF ---' AS mode;
SET GLOBAL mysql_experimental_filter_pushdown=false;
CALL mysql_clear_cache();

SELECT SUM(id) FROM s1.bench_speed WHERE indexed_col = 0;
SELECT SUM(id) FROM s1.bench_speed WHERE indexed_col = 0;
SELECT SUM(id) FROM s1.bench_speed WHERE indexed_col = 0;

SELECT '=== BENCHMARK 8: Full Table Scan — no WHERE (50K rows) ===' AS benchmark;

SELECT '--- Federation ON ---' AS mode;
SET GLOBAL mysql_experimental_filter_pushdown=true;
CALL mysql_clear_cache();

SELECT COUNT(id) FROM s1.bench_speed WHERE indexed_col >= 0;
SELECT COUNT(id) FROM s1.bench_speed WHERE indexed_col >= 0;
SELECT COUNT(id) FROM s1.bench_speed WHERE indexed_col >= 0;

SELECT '--- Federation OFF ---' AS mode;
SET GLOBAL mysql_experimental_filter_pushdown=false;
CALL mysql_clear_cache();

SELECT COUNT(id) FROM s1.bench_speed WHERE indexed_col >= 0;
SELECT COUNT(id) FROM s1.bench_speed WHERE indexed_col >= 0;
SELECT COUNT(id) FROM s1.bench_speed WHERE indexed_col >= 0;

SELECT '=== CLEANUP ===' AS info;
DROP TABLE IF EXISTS s1.bench_speed;

SET GLOBAL mysql_experimental_filter_pushdown=true;
