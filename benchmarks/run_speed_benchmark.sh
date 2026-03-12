#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DUCKDB="$PROJECT_DIR/build/debug/duckdb"

export MYSQL_TEST_DATABASE_AVAILABLE=1
export LOCAL_EXTENSION_REPO="$PROJECT_DIR/build/debug/extension"

if [ ! -f "$DUCKDB" ]; then
    echo "ERROR: DuckDB binary not found at $DUCKDB"
    echo "Run: GEN=ninja make debug"
    exit 1
fi

# Read connection info from .cnf file if provided
# Usage: MYSQL_CNF=~/.my.cnf bash benchmarks/run_speed_benchmark.sh
#        MYSQL_CNF=/path/to/remote.cnf bash benchmarks/run_speed_benchmark.sh
#
# Expected .cnf format (standard MySQL option file):
#   [client]
#   host=remote-host.example.com
#   user=myuser
#   password=mypass
#   port=3306
#   database=mydb
if [ -n "${MYSQL_CNF:-}" ] && [ -f "$MYSQL_CNF" ]; then
    read_cnf_value() {
        local key="$1"
        # Parse [client] section, handle key=value and key = value
        awk -v key="$key" '
            /^\[client\]/ { in_section=1; next }
            /^\[/ { in_section=0 }
            in_section && $0 ~ "^\\s*" key "\\s*=" {
                sub(/^[^=]*=\s*/, "")
                sub(/\s*$/, "")
                print
                exit
            }
        ' "$MYSQL_CNF"
    }
    MYSQL_HOST="${MYSQL_HOST:-$(read_cnf_value host)}"
    MYSQL_USER="${MYSQL_USER:-$(read_cnf_value user)}"
    MYSQL_PASS="${MYSQL_PASS:-$(read_cnf_value password)}"
    MYSQL_PORT="${MYSQL_PORT:-$(read_cnf_value port)}"
    MYSQL_DB="${MYSQL_DB:-$(read_cnf_value database)}"
fi

MYSQL_HOST="${MYSQL_HOST:-localhost}"
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PORT="${MYSQL_PORT:-0}"
MYSQL_DB="${MYSQL_DB:-mysqlscanner}"
MYSQL_PASS="${MYSQL_PASS:-}"

BENCH_DB="mysqlscanner_bench"

ADMIN_CONN="host=$MYSQL_HOST user=$MYSQL_USER port=$MYSQL_PORT database=mysql"
CONN_STRING="host=$MYSQL_HOST user=$MYSQL_USER port=$MYSQL_PORT database=$BENCH_DB"
if [ -n "$MYSQL_PASS" ]; then
    ADMIN_CONN="$ADMIN_CONN password=$MYSQL_PASS"
    CONN_STRING="$CONN_STRING password=$MYSQL_PASS"
fi

ROWS="${ROWS:-50000}"
ITERATIONS="${ITERATIONS:-5}"

echo "================================================================="
echo "  Federation Speed Benchmark"
echo "================================================================="
echo "  MySQL:      $MYSQL_HOST:$MYSQL_PORT/$BENCH_DB"
echo "  Rows:       $ROWS"
echo "  Iterations: $ITERATIONS per query"
echo "================================================================="
echo ""

create_database() {
    echo "Creating database $BENCH_DB..."
    "$DUCKDB" -unsigned -csv -noheader -c "
        ATTACH '$ADMIN_CONN' AS admin (TYPE MYSQL_SCANNER);
        CALL mysql_execute('admin', 'CREATE DATABASE IF NOT EXISTS $BENCH_DB');
        SELECT 'ok';
    " 2>/dev/null
}

drop_database() {
    echo "Dropping database $BENCH_DB..."
    "$DUCKDB" -unsigned -csv -noheader -c "
        ATTACH '$ADMIN_CONN' AS admin (TYPE MYSQL_SCANNER);
        CALL mysql_execute('admin', 'DROP DATABASE IF EXISTS $BENCH_DB');
        SELECT 'ok';
    " 2>/dev/null
}

setup_data() {
    create_database
    echo "Setting up test data ($ROWS rows)..."
    "$DUCKDB" -unsigned -csv -noheader -c "
        ATTACH '$CONN_STRING' AS loader (TYPE MYSQL_SCANNER);
        DETACH loader;
        ATTACH '$CONN_STRING' AS s1 (TYPE MYSQL_SCANNER);

        CALL mysql_execute('s1', 'DROP TABLE IF EXISTS bench_detail');
        CALL mysql_execute('s1', 'DROP TABLE IF EXISTS bench_speed');

        CALL mysql_execute('s1', '
            CREATE TABLE bench_speed (
                id INTEGER NOT NULL PRIMARY KEY,
                indexed_col INTEGER NOT NULL,
                status VARCHAR(20) NOT NULL,
                nullable_col INTEGER,
                payload VARCHAR(500) NOT NULL,
                INDEX idx_bench_indexed (indexed_col),
                INDEX idx_bench_status (status(20))
            )');

        CALL mysql_execute('s1', '
            CREATE TABLE bench_detail (
                detail_id INTEGER NOT NULL PRIMARY KEY,
                speed_id INTEGER NOT NULL,
                category VARCHAR(20) NOT NULL,
                value INTEGER NOT NULL,
                INDEX idx_detail_speed_id (speed_id),
                INDEX idx_detail_category (category(20))
            )');

        CALL mysql_clear_cache();

        INSERT INTO s1.bench_speed
        SELECT
            range AS id,
            range % 100 AS indexed_col,
            CASE WHEN range % 3 = 0 THEN 'active'
                 WHEN range % 3 = 1 THEN 'pending'
                 ELSE 'closed' END,
            CASE WHEN range % 10 = 0 THEN NULL ELSE range % 50 END,
            REPEAT('x', 200)
        FROM range($ROWS);

        INSERT INTO s1.bench_detail
        SELECT
            range AS detail_id,
            range % $ROWS AS speed_id,
            CASE WHEN range % 4 = 0 THEN 'typeA'
                 WHEN range % 4 = 1 THEN 'typeB'
                 WHEN range % 4 = 2 THEN 'typeC'
                 ELSE 'typeD' END,
            range % 1000
        FROM range($ROWS * 2);

        CALL mysql_execute('s1', 'ANALYZE TABLE bench_speed');
        CALL mysql_execute('s1', 'ANALYZE TABLE bench_detail');
        SELECT 'ok';
    " 2>/dev/null
    echo "Setup complete."
    echo ""
}

cleanup_data() {
    drop_database
}

time_query() {
    local mode="$1"
    local query="$2"
    local iters="$3"

    local timed_block=""
    for i in $(seq 1 "$iters"); do
        timed_block+="SELECT epoch_ms(get_current_timestamp());"
        timed_block+="$query;"
        timed_block+="SELECT epoch_ms(get_current_timestamp());"
    done

    local output
    output=$("$DUCKDB" -unsigned -csv -noheader -c "
        ATTACH '$CONN_STRING' AS loader (TYPE MYSQL_SCANNER);
        DETACH loader;
        SET GLOBAL mysql_experimental_filter_pushdown=$mode;
        ATTACH '$CONN_STRING' AS s1 (TYPE MYSQL_SCANNER);
        CALL mysql_clear_cache();
        $query;
        $query;
        $timed_block
    " 2>/dev/null)

    echo "$output" | python3 -c "
import sys
lines = [l.strip() for l in sys.stdin if l.strip()]
timestamps = []
for l in lines:
    try:
        v = int(l)
        if v > 1700000000000:
            timestamps.append(v)
    except (ValueError, OverflowError):
        pass

if len(timestamps) < 2:
    print('ERROR,ERROR,ERROR')
    sys.exit(0)

times_ms = []
for i in range(0, len(timestamps) - 1, 2):
    delta = timestamps[i+1] - timestamps[i]
    times_ms.append(delta)

if not times_ms:
    print('ERROR,ERROR,ERROR')
else:
    print(f'{min(times_ms):.0f},{sum(times_ms)/len(times_ms):.1f},{max(times_ms):.0f}')
"
}

run_benchmark() {
    local label="$1"
    local query="$2"

    local on_result off_result
    on_result=$(time_query "true" "$query" "$ITERATIONS")
    off_result=$(time_query "false" "$query" "$ITERATIONS")

    local on_min on_avg on_max off_min off_avg off_max
    IFS=',' read -r on_min on_avg on_max <<< "$on_result"
    IFS=',' read -r off_min off_avg off_max <<< "$off_result"

    if [[ "$on_avg" != "ERROR" && "$off_avg" != "ERROR" ]]; then
        local speedup
        speedup=$(python3 -c "
on = float('$on_avg')
off = float('$off_avg')
if on > 0:
    print(f'{off/on:.1f}x')
elif off > 0:
    print('INF')
else:
    print('1.0x')
")
        printf "%-28s │ %7s │ %7s │ %7s │ %7s │ %7s │ %7s │ %7s\n" \
            "$label" "${on_min}ms" "${on_avg}ms" "${on_max}ms" "${off_min}ms" "${off_avg}ms" "${off_max}ms" "$speedup"
    else
        printf "%-28s │ %7s │ %7s │ %7s │ %7s │ %7s │ %7s │ %7s\n" \
            "$label" "ERR" "ERR" "ERR" "ERR" "ERR" "ERR" "N/A"
    fi
}

setup_data

printf "\n"
printf "%-28s │ %-25s │ %-25s │ %7s\n" "" "    Federation ON (ms)" "    Federation OFF (ms)" ""
printf "%-28s │ %7s │ %7s │ %7s │ %7s │ %7s │ %7s │ %7s\n" \
    "Query" "min" "avg" "max" "min" "avg" "max" "Speedup"
printf "%-28s─┼─%7s─┼─%7s─┼─%7s─┼─%7s─┼─%7s─┼─%7s─┼─%7s\n" \
    "$(printf '─%.0s' {1..28})" "───────" "───────" "───────" "───────" "───────" "───────" "───────"

run_benchmark "PK Point Lookup (1 row)"     "SELECT id FROM s1.bench_speed WHERE id = 25000"
run_benchmark "Indexed Eq 1% (500 rows)"    "SELECT id FROM s1.bench_speed WHERE indexed_col = 42"
run_benchmark "Compound 0.3% (~170 rows)"   "SELECT id FROM s1.bench_speed WHERE indexed_col = 10 AND status = 'active'"
run_benchmark "Range 10% (5K rows)"          "SELECT id FROM s1.bench_speed WHERE indexed_col BETWEEN 10 AND 19"
run_benchmark "OR predicate (1K rows)"       "SELECT id FROM s1.bench_speed WHERE indexed_col = 10 OR indexed_col = 42"
run_benchmark "IN list (2.5K rows)"          "SELECT id FROM s1.bench_speed WHERE indexed_col IN (1, 5, 10, 15, 20)"
run_benchmark "LIKE prefix (33%)"            "SELECT id FROM s1.bench_speed WHERE status LIKE 'act%'"
run_benchmark "IS NULL (~10%)"               "SELECT id FROM s1.bench_speed WHERE nullable_col IS NULL"
run_benchmark "IS NOT NULL (~90%)"           "SELECT id FROM s1.bench_speed WHERE nullable_col IS NOT NULL"
run_benchmark "COUNT(*) full table"          "SELECT COUNT(*) FROM s1.bench_speed"
run_benchmark "COUNT(*) + WHERE"             "SELECT COUNT(*) FROM s1.bench_speed WHERE indexed_col = 77"
run_benchmark "SUM(id) + WHERE"             "SELECT SUM(id) FROM s1.bench_speed WHERE indexed_col = 0"
run_benchmark "GROUP BY + HAVING"            "SELECT indexed_col, COUNT(*) AS cnt FROM s1.bench_speed GROUP BY indexed_col HAVING COUNT(*) > 500"
run_benchmark "ORDER BY + LIMIT 5"          "SELECT id FROM s1.bench_speed ORDER BY indexed_col, id LIMIT 5"
run_benchmark "Large result + LIMIT"         "SELECT * FROM s1.bench_speed WHERE indexed_col > 0 ORDER BY id LIMIT 100"
run_benchmark "JOIN + filter"                "SELECT s.id, d.category FROM s1.bench_speed s JOIN s1.bench_detail d ON s.id = d.speed_id WHERE s.indexed_col = 42"
run_benchmark "Semi-join IN subquery"        "SELECT id FROM s1.bench_speed WHERE id IN (SELECT speed_id FROM s1.bench_detail WHERE category = 'typeA')"
run_benchmark "Full scan (baseline)"         "SELECT COUNT(id) FROM s1.bench_speed WHERE indexed_col >= 0"

echo ""
printf "%-28s─┴─%7s─┴─%7s─┴─%7s─┴─%7s─┴─%7s─┴─%7s─┴─%7s\n" \
    "$(printf '─%.0s' {1..28})" "───────" "───────" "───────" "───────" "───────" "───────" "───────"
echo ""
echo "Speedup = OFF avg / ON avg (higher = federation is faster)"
echo "ON  = federation enabled (filter pushdown, cost model, aggregate pushdown)"
echo "OFF = federation disabled (scan all rows from MySQL, filter/aggregate in DuckDB)"
echo ""
echo "To test with simulated network latency (Linux):"
echo "  sudo tc qdisc add dev lo root netem delay 5ms"
echo "  ROWS=100000 $0"
echo "  sudo tc qdisc del dev lo root"
echo ""

cleanup_data
echo "Benchmark complete."
