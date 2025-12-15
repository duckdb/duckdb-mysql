# name: test/sql/insert_empy_blob.test
# description: Test inserting empy blob
# group: [sql]

require mysql_scanner

require-env MYSQL_TEST_DATABASE_AVAILABLE

statement ok
ATTACH 'host=localhost user=root port=0 database=mysqlscanner' AS msql (TYPE MYSQL_SCANNER)

statement ok
USE msql

statement ok
CREATE OR REPLACE TABLE empty_blob_tbl(v BLOB);

statement ok
INSERT INTO empty_blob_tbl VALUES (''::BLOB)

query I
SELECT * FROM blob_tbl
----
(empty)