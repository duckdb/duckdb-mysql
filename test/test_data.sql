DROP SCHEMA IF EXISTS mysqlscanner;
CREATE SCHEMA mysqlscanner;
USE mysqlscanner;

CREATE TABLE booleans(b BOOLEAN);
INSERT INTO booleans VALUES (false), (true), (NULL);

CREATE TABLE signed_integers(t TINYINT, s SMALLINT, m MEDIUMINT, i INTEGER, b BIGINT);
INSERT INTO signed_integers VALUES (
	-128,
	-32768,
	-8388608,
	-2147483648,
	-9223372036854775808
);
INSERT INTO signed_integers VALUES (
	127,
	32767,
	8388607,
	2147483647,
	9223372036854775807
);
INSERT INTO signed_integers VALUES (
	NULL, NULL, NULL, NULL, NULL
);

CREATE TABLE unsigned_integers(t TINYINT UNSIGNED, s SMALLINT UNSIGNED, m MEDIUMINT UNSIGNED, i INT UNSIGNED, b BIGINT UNSIGNED);
INSERT INTO unsigned_integers VALUES (
	0, 0, 0, 0, 0
);
INSERT INTO unsigned_integers VALUES (
	255,
	65535,
	16777215,
	4294967295,
	18446744073709551615
);
INSERT INTO unsigned_integers VALUES (
	NULL, NULL, NULL, NULL, NULL
);

CREATE TABLE floating_points(f FLOAT, d DOUBLE, fu FLOAT UNSIGNED, du DOUBLE UNSIGNED);
INSERT INTO floating_points VALUES (
	0, 0, 0, 0
);
INSERT INTO floating_points VALUES (
	0.5, 0.5, 0.5, 0.5
);
INSERT INTO floating_points VALUES (
	-0.5, -0.5, 0, 0
);
INSERT INTO floating_points VALUES (
	NULL, NULL, NULL, NULL
);

CREATE TABLE zero_fill_integers(t INT(4) ZEROFILL);
INSERT INTO zero_fill_integers VALUES (0), (2147483647);
INSERT INTO zero_fill_integers VALUES (NULL);

CREATE TABLE decimals(xs DECIMAL(2, 1), s DECIMAL(5, 1), m DECIMAL(10, 2), l DECIMAL(20, 3), xl DECIMAL(40, 4));
INSERT INTO decimals VALUES (
	0.5,
	1234.1,
	12345678.12,
	12345678901234567.123,
	123456789012345678901234567890123456.1234
);
INSERT INTO decimals VALUES (
	-0.5,
	-1234.1,
	-12345678.12,
	-12345678901234567.123,
	-123456789012345678901234567890123456.1234
);
INSERT INTO decimals VALUES (
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
);

-- MySQL doesn't support a "proper" boolean so people often use TINYINT(1) or BIT(1) as boolean
CREATE TABLE fake_booleans(tinyint_bool TINYINT(1), bit_bool BIT(1));
INSERT INTO fake_booleans VALUES (true, true), (false, false), (NULL, NULL), (-128, b'1'), (127, b'0');

CREATE TABLE bits(b BIT(6), bl BIT(64));
INSERT INTO bits VALUES (b'000101', b'010101010101010101');
INSERT INTO bits VALUES (NULL, NULL);

SET SESSION time_zone = '-05:00';
CREATE TABLE datetime_tbl(id INTEGER, d DATE, date_time DATETIME, ts TIMESTAMP, t TIME, y YEAR);
INSERT INTO datetime_tbl VALUES (1, '2020-02-03', '2029-02-14 08:47:23', '2029-02-14 08:47:23', '23:59:59', '1901');
INSERT INTO datetime_tbl VALUES (2, '1000-01-01', '1000-01-01 00:00:00.000000', '1970-01-01 00:00:01.000000', '-838:59:59', '2155');
INSERT INTO datetime_tbl VALUES (3, '9999-12-31', '9999-12-31 23:59:59.499999', '2038-01-18 22:14:07.499999', '838:59:59', '2000');
INSERT INTO datetime_tbl VALUES (4, NULL, NULL, NULL, NULL, NULL);

CREATE TABLE text_tbl(v VARCHAR(4), c CHAR(4), t TEXT);
INSERT INTO text_tbl VALUES ('ab  ', 'ab  ', 'thisisalongstring');
INSERT INTO text_tbl VALUES ('', '', '');
INSERT INTO text_tbl VALUES ('🦆', '🦆', '🦆🦆🦆🦆');
INSERT INTO text_tbl VALUES (NULL, NULL, NULL);

CREATE TABLE blob_tbl(bi BINARY(4), vbi VARBINARY(4), bl BLOB);
INSERT INTO blob_tbl VALUES ('c\0\0', 'c\0\0', 'c\0\0');
INSERT INTO blob_tbl VALUES ('', '', '');
INSERT INTO blob_tbl VALUES (0x80, 0x80, 0x80);
INSERT INTO blob_tbl VALUES (NULL, NULL, NULL);

CREATE TABLE enum_tbl (
    size ENUM('x-small', 'small', 'medium', 'large', 'x-large')
);
INSERT INTO enum_tbl VALUES ('x-small'), ('small'), ('medium'), ('large'), ('x-large'), (NULL);

CREATE TABLE set_tbl (col SET('a', 'b', 'c', 'd'));
INSERT INTO set_tbl (col) VALUES ('a,d'), ('d,a'), ('a,d,a'), ('a,d,d'), ('d,a,d');

CREATE TABLE json_tbl (col JSON);
INSERT INTO json_tbl (col) VALUES ('{"k1": "value", "k2": 10}'), ('["abc", 10, null, true, false]'), (NULL);

CREATE TABLE `latin_unicode` (
  `id_user` int NOT NULL AUTO_INCREMENT,
  `name` varchar(45) NOT NULL,
  PRIMARY KEY (`id_user`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
INSERT INTO latin_unicode (name) VALUES ('Graça');

SET sql_mode = '';
CREATE TABLE zero_date(d DATE, date_time DATETIME, ts TIMESTAMP, t TIME, y YEAR);
INSERT INTO zero_date VALUES ('0000-00-00', '0000-00-00 00:00:00', '0000-00-00 00:00:00', '00:00:00', '0000');

CREATE TABLE geom (
	g GEOMETRY,
	p POINT,
    ls LINESTRING,
    poly POLYGON,
    mp MULTIPOINT,
    mls MULTILINESTRING,
    mpoly MULTIPOLYGON,
    gc GEOMETRYCOLLECTION);
INSERT INTO geom
VALUES (
	ST_GeomFromText('POINT(1 1)'),
	ST_GeomFromText('POINT(1 1)'),
	ST_GeomFromText('LINESTRING(2 1, 6 6)'),
	ST_GeomFromText('POLYGON((0 5, 2 5, 2 7, 0 7, 0 5))'),
	ST_GeomFromText('MULTIPOINT((1 1))'),
	ST_GeomFromText('MULTILINESTRING((2 1, 6 6))'),
	ST_GeomFromText('MULTIPOLYGON(((0 5, 2 5, 2 7, 0 7, 0 5)))'),
	ST_GeomFromText('GEOMETRYCOLLECTION EMPTY')
);

CREATE TABLE tbl_issue65 (col1 int, col2 decimal(5,2));
insert into tbl_issue65 values (1,1.11), (2,2.22), (3,3.33);
