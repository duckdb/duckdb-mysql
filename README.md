# DuckDB MySQL extension

The MySQL extension allows DuckDB to directly read and write data from a MySQL database instance. The data can be queried directly from the underlying MySQL database. Data can be loaded from MySQL tables into DuckDB tables, or vice versa.

## Reading Data from MySQL


To make a MySQL database accessible to DuckDB use the `ATTACH` command:

```sql
ATTACH 'host=localhost user=root port=0 database=mysqlscanner' AS mysqlscanner (TYPE mysql_scanner);
USE mysqlscanner;
```

The connection string determines the parameters for how to connect to MySQL as a set of `key=value` pairs. Any options not provided are read from the corresponding environment variables if set, and otherwise replaced by their default values, as per the table below.

| Setting  |        Description         | Environment Variable |   Default    |
|----------|----------------------------|----------------------|--------------|
| host     | Name of host to connect to | `MYSQL_HOST`           | localhost    |
| user     | MySQL user name            | `MYSQL_USER`           | current_user |
| password | MySQL password             | `MYSQL_PWD`            |              |
| database | Database name              | `MYSQL_DATABASE`       | NULL         |
| port     | Port number                | `MYSQL_TCP_PORT`       | 0            |
| socket   | Unix socket file name      | `MYSQL_UNIX_PORT`      | NULL         |
| compress | Compress MySQL packet      | `MYSQL_COMPRESS`       | 1            |


The tables in the file can be read as if they were normal DuckDB tables, but the underlying data is read directly from MySQL at query time.

```sql
D SHOW TABLES;
┌───────────────────────────────────────┐
│                 name                  │
│                varchar                │
├───────────────────────────────────────┤
│ signed_integers                       │
└───────────────────────────────────────┘
D SELECT * FROM signed_integers;
┌──────┬────────┬──────────┬─────────────┬──────────────────────┐
│  t   │   s    │    m     │      i      │          b           │
│ int8 │ int16  │  int32   │    int32    │        int64         │
├──────┼────────┼──────────┼─────────────┼──────────────────────┤
│ -128 │ -32768 │ -8388608 │ -2147483648 │ -9223372036854775808 │
│  127 │  32767 │  8388607 │  2147483647 │  9223372036854775807 │
│ NULL │   NULL │     NULL │        NULL │                 NULL │
└──────┴────────┴──────────┴─────────────┴──────────────────────┘
```

It might be desirable to create a copy of the MySQL databases in DuckDB to prevent the system from re-reading the tables from MySQL continuously, particularly for large tables.

Data can be copied over from MySQL to DuckDB using standard SQL, for example:

```sql
CREATE TABLE duckdb_table AS FROM mysqlscanner.mysql_table;
```

## Writing Data to MySQL

In addition to reading data from MySQL, create tables, ingest data into MySQL and make other modifications to a MySQL database using standard SQL queries.

This allows you to use DuckDB to, for example, export data that is stored in a MySQL database to Parquet, or read data from a Parquet file into MySQL.

Below is a brief example of how to create a new table in MySQL and load data into it.

```sql
ATTACH 'host=localhost user=root port=0 database=mysqlscanner' AS mysql_db (TYPE mysql_scanner);
CREATE TABLE mysql_db.tbl(id INTEGER, name VARCHAR);
INSERT INTO mysql_db.tbl VALUES (42, 'DuckDB');
```
Many operations on MySQL tables are supported. All these operations directly modify the MySQL database, and the result of subsequent operations can then be read using MySQL.
Note that if modifications are not desired, `ATTACH` can be run with the `READ_ONLY` property which prevents making modifications to the underlying database. For example:

```sql
ATTACH 'host=localhost user=root port=0 database=mysqlscanner' AS mysql_db (TYPE mysql_scanner, READ_ONLY);
```

Below is a list of supported operations.

###### CREATE TABLE
```sql
CREATE TABLE mysql_db.tbl(id INTEGER, name VARCHAR);
```

###### INSERT INTO
```sql
INSERT INTO mysql_db.tbl VALUES (42, 'DuckDB');
```

###### SELECT
```sql
SELECT * FROM mysql_db.tbl;
┌───────┬─────────┐
│  id   │  name   │
│ int64 │ varchar │
├───────┼─────────┤
│    42 │ DuckDB  │
└───────┴─────────┘
```

###### COPY
```sql
COPY mysql_db.tbl TO 'data.parquet';
COPY mysql_db.tbl FROM 'data.parquet';
```

###### UPDATE
```sql
UPDATE mysql_db.tbl SET name='Woohoo' WHERE id=42;
```

###### DELETE
```sql
DELETE FROM mysql_db.tbl WHERE id=42;
```

###### ALTER TABLE
```sql
ALTER TABLE mysql_db.tbl ADD COLUMN k INTEGER;
```

###### DROP TABLE
```sql
DROP TABLE mysql_db.tbl;
```

###### CREATE VIEW
```sql
CREATE VIEW mysql_db.v1 AS SELECT 42;
```

###### CREATE SCHEMA/DROP SCHEMA
```sql
CREATE SCHEMA mysql_db.s1;
CREATE TABLE mysql_db.s1.integers(i int);
INSERT INTO mysql_db.s1.integers VALUES (42);
SELECT * FROM mysql_db.s1.integers;
┌───────┐
│   i   │
│ int32 │
├───────┤
│    42 │
└───────┘
DROP SCHEMA mysql_db.s1;
```

###### Transactions
```sql
CREATE TABLE mysql_db.tmp(i INTEGER);
BEGIN;
INSERT INTO mysql_db.tmp VALUES (42);
SELECT * FROM mysql_db.tmp;
┌───────┐
│   i   │
│ int64 │
├───────┤
│    42 │
└───────┘
ROLLBACK;
SELECT * FROM mysql_db.tmp;
┌────────┐
│   i    │
│ int64  │
├────────┤
│ 0 rows │
└────────┘
```

> Note that DDL statements are not transactional in MySQL.

## Settings
|                name                |                          description                           | default |
|------------------------------------|----------------------------------------------------------------|---------|
| mysql_experimental_filter_pushdown | Whether or not to use filter pushdown (currently experimental) | true    |
| mysql_tinyint1_as_boolean          | Whether or not to convert TINYINT(1) columns to BOOLEAN        | true    |
| mysql_debug_show_queries           | DEBUG SETTING: print all queries sent to MySQL to stdout       | false   |
| mysql_bit1_as_boolean              | Whether or not to convert BIT(1) columns to BOOLEAN            | true    |

## Schema Cache

To avoid having to continuously fetch schema data from MySQL, DuckDB keeps schema information - such as the names of tables, their columns, etc -  cached. If changes are made to the schema through a different connection to the MySQL instance, such as new columns being added to a table, the cached schema information might be outdated. In this case, the function `mysql_clear_cache` can be executed to clear the internal caches.

```sql
CALL mysql_clear_cache();
```

## Development

#### Dependencies

The package depends on `vcpkg`, and has several platform-specific dependencies that must be installed in order for compilation to succeed.

##### Submodules

The DuckDB submodule must be initialized prior to building.

```bash
git submodule init
git pull --recurse-submodules
```

##### vcpkg

`vcpkg` must be installed and configured for building. For more information, see [here](https://github.com/duckdb/extension-template/tree/main#managing-dependencies).

```bash
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```

##### Ubuntu

```bash
sudo apt-get install -y ninja-build cmake build-essential make ccache curl zip unzip tar
sudo apt-get install -y pkg-config autoconf autoconf-archive
```

##### MacOS

```bash
brew install pkg-config ninja automake autoconf autoconf-archive libevent
```

## Building & Loading the Extension
To build, type:

```
make
```

To run, run the bundled `duckdb` shell:
```
 ./build/release/duckdb -unsigned
```

Then, load the MySQL extension like so:
```SQL
LOAD 'build/release/extension/mysql_scanner/mysql_scanner.duckdb_extension';
```

## Testing

Tests can be run with the following command:

```bash
make test
```

Note that most test will require to have a mysql server running to actually run. To run these tests, setup the mysql server
and set the environment variable `MYSQL_TEST_DATABASE_AVAILABLE=1`. 
