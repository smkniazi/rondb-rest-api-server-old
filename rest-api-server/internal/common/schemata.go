package common

import (
	"strconv"
	"strings"
)

/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2022 Hopsworks AB
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 3.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

var databases map[string][][]string = make(map[string][][]string)

func init() {
	db := "DB001"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,

			"CREATE TABLE table_1(id0 VARCHAR(10), col_0 VARCHAR(100), col_1 VARCHAR(100), col_2 VARCHAR(100), PRIMARY KEY(id0))",
			"INSERT INTO table_1 VALUES('id0_data', 'col_0_data', 'col_1_data', 'col_2_data')",
		},

		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	db = "DB002"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,

			"CREATE TABLE table_1(id0 VARCHAR(10), id1 VARCHAR(10), col_0 VARCHAR(100), col_1 VARCHAR(100), col_2 VARCHAR(100), PRIMARY KEY(id0, id1))",
			"INSERT INTO table_1 VALUES('id0_data', 'id1_data', 'col_0_data', 'col_1_data', 'col_2_data')",
		},

		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	db = "DB003"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,

			"CREATE TABLE `date_table` ( `id0` int NOT NULL, `col0` date DEFAULT NULL, `col1` time DEFAULT NULL, `col2` datetime DEFAULT NULL, `col3` timestamp NULL DEFAULT NULL, `col4` year DEFAULT NULL, PRIMARY KEY (`id0`))",
			"insert into date_table values(1, \"1111-11-11\", \"11:11:11\", \"1111-11-11 11:11:11\", \"1970-11-11 11:11:11\", \"11\")",
			"insert into date_table set id0=2",

			"CREATE TABLE `arrays_table` ( `id0` int NOT NULL, `col0` char(100) DEFAULT NULL, `col2` varchar(100) DEFAULT NULL, `col3` binary(100) DEFAULT NULL, `col4` varbinary(100)      DEFAULT NULL, PRIMARY KEY (`id0`))",
			"insert into arrays_table values (1, \"abcd\", \"abcd\", 0xFFFF, 0xFFFF)",
			"insert into arrays_table set id0=2",

			"CREATE TABLE `set_table` ( `id0` int NOT NULL, `col0` enum('a','b','c','d') DEFAULT NULL, `col1` set('a','b','c','d') DEFAULT NULL, PRIMARY KEY (`id0`))",
			"INSERT INTO `set_table` VALUES (1,'a','a')",
			"INSERT INTO `set_table` VALUES (2,'b','a,b')",
			"insert into set_table set id0=3",

			"CREATE TABLE `special_table` ( `id0` int NOT NULL, `col0` geometry DEFAULT NULL, `col1` point DEFAULT NULL, `col2` linestring DEFAULT NULL, `col3` polygon DEFAULT NULL,       `col4` geomcollection DEFAULT NULL, `col5` multilinestring DEFAULT NULL, `col6` multipoint DEFAULT NULL, `col7` multipolygon DEFAULT NULL, PRIMARY KEY (`id0`))",
			"insert into special_table set id0=1, col0=ST_GeomFromText('POINT(1 1)'), col1=ST_GeomFromText('POINT(1 1)'), col2=ST_GeomFromText('LineString(1 1,2 2,3 3)'), col3=ST_GeomFromText('Polygon((0 0,0 3,3 0,0 0),(1 1,1 2,2 1,1 1))'), col7=ST_GeomFromText('MultiPolygon(((0 0,0 3,3 3,3 0,0 0),(1 1,1 2,2 2,2 1,1 1)))'),col4=ST_GeomFromText('GeometryCollection(Point(1 1),LineString(2 2, 3 3))'),col6=ST_MPointFromText('MULTIPOINT (1 1, 2 2, 3 3)'),col5=ST_GeomFromText('MultiLineString((1 1,2 2,3 3),(4 4,5 5))')",
			"insert into special_table set id0=2",

			"CREATE TABLE `number_table` ( `id0` int NOT NULL, `col0` tinyint DEFAULT NULL, `col1` smallint DEFAULT NULL, `col2` mediumint DEFAULT NULL, `col3` int DEFAULT NULL, `col4` bigint DEFAULT NULL, `col5` decimal(10, 0) DEFAULT NULL, `col6` float DEFAULT NULL, `col7` double DEFAULT NULL, `col8` bit(1) DEFAULT NULL, PRIMARY KEY (`id0`))",
			"INSERT INTO `number_table` VALUES (1,99,99,99,99,99,99,99.99,99.99,true)",
			"insert into number_table set id0=2",

			"CREATE TABLE `blob_table` ( `id0` int NOT NULL, `col0` tinyblob, `col1` blob, `col2` mediumblob, `col3` longblob, `col4` tinytext, `col5` mediumtext, `col6` longtext, PRIMARY KEY (`id0`))",
			"insert into blob_table values(1, 0xFFFF, 0xFFFF, 0xFFFF,  0xFFFF, \"abcd\", \"abcd\", \"abcd\")",
			"insert into blob_table set id0=2",
		},

		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	// signed and unsigned number data types
	db = "DB004"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,

			"CREATE TABLE int_table(id0 INT, id1 INT UNSIGNED, col0 INT, col1 INT UNSIGNED, PRIMARY KEY(id0, id1))",
			"INSERT INTO  int_table VALUES(2147483647,4294967295,2147483647,4294967295)",
			"INSERT INTO  int_table VALUES(-2147483648,0,-2147483648,0)",
			"INSERT INTO  int_table VALUES(0,0,0,0)",
			"INSERT INTO  int_table set id0=1, id1=1", // NULL values for non primary columns

			// this table only has primary keys
			"CREATE TABLE int_table1(id0 INT, id1 INT UNSIGNED, PRIMARY KEY(id0, id1))",
			"INSERT INTO  int_table1 VALUES(0,0)",
		},

		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	db = "DB005"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,

			"CREATE TABLE bigint_table(id0 BIGINT, id1 BIGINT UNSIGNED, col0 BIGINT, col1 BIGINT UNSIGNED, PRIMARY KEY(id0, id1))",
			"INSERT INTO  bigint_table VALUES(9223372036854775807,18446744073709551615,9223372036854775807,18446744073709551615)",
			"INSERT INTO  bigint_table VALUES(-9223372036854775808,0,-9223372036854775808,0)",
			"INSERT INTO  bigint_table VALUES(0,0,0,0)",
			"INSERT INTO  bigint_table set id0=1, id1=1", // NULL values for non primary columns
		},

		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	db = "DB006"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,

			"CREATE TABLE tinyint_table(id0 TINYINT, id1 TINYINT UNSIGNED, col0 TINYINT, col1 TINYINT UNSIGNED, PRIMARY KEY(id0, id1))",
			"INSERT INTO  tinyint_table VALUES(127,255,127,255)",
			"INSERT INTO  tinyint_table VALUES(-128,0,-128,0)",
			"INSERT INTO  tinyint_table VALUES(0,0,0,0)",
			"INSERT INTO  tinyint_table set id0=1, id1=1", // NULL values for non primary columns
		},

		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	db = "DB007"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,

			"CREATE TABLE smallint_table(id0 SMALLINT, id1 SMALLINT UNSIGNED, col0 SMALLINT, col1 SMALLINT UNSIGNED, PRIMARY KEY(id0, id1))",
			"INSERT INTO  smallint_table VALUES(32767,65535,32767,65535)",
			"INSERT INTO  smallint_table VALUES(-32768,0,-32768,0)",
			"INSERT INTO  smallint_table VALUES(0,0,0,0)",
			"INSERT INTO  smallint_table set id0=1, id1=1", // NULL values for non primary columns

		},

		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	db = "DB008"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,

			"CREATE TABLE mediumint_table(id0 MEDIUMINT, id1 MEDIUMINT UNSIGNED, col0 MEDIUMINT, col1 MEDIUMINT UNSIGNED, PRIMARY KEY(id0, id1))",
			"INSERT INTO  mediumint_table VALUES(8388607,16777215,8388607,16777215)",
			"INSERT INTO  mediumint_table VALUES(-8388608,0,-8388608,0)",
			"INSERT INTO  mediumint_table VALUES(0,0,0,0)",
			"INSERT INTO  mediumint_table set id0=1, id1=1", // NULL values for non primary columns

		},

		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	db = "DB009"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,

			"CREATE TABLE float_table1(id0 INT, col0 FLOAT, col1 FLOAT UNSIGNED, PRIMARY KEY(id0))",
			"INSERT INTO  float_table1 VALUES(1,-123.123,123.123)",
			"INSERT INTO  float_table1 VALUES(0,0,0)",
			"INSERT INTO  float_table1 set id0=2", // NULL values for non primary columns

			"CREATE TABLE float_table2(id0 FLOAT, col0 FLOAT, col1 FLOAT UNSIGNED, PRIMARY KEY(id0))",
		},

		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	db = "DB010"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,

			"CREATE TABLE double_table1(id0 INT, col0 DOUBLE, col1 DOUBLE UNSIGNED, PRIMARY KEY(id0))",
			"INSERT INTO  double_table1 VALUES(1,-123.123,123.123)",
			"INSERT INTO  double_table1 VALUES(0,0,0)",
			"INSERT INTO  double_table1 set id0=2", // NULL values for non primary columns

			"CREATE TABLE double_table2(id0 DOUBLE, col0 DOUBLE, col1 DOUBLE UNSIGNED, PRIMARY KEY(id0))",
		},

		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	db = "DB011"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,

			"CREATE TABLE decimal_table(id0 DECIMAL(10,5), id1 DECIMAL(10,5) UNSIGNED, col0 DECIMAL(10,5), col1 DECIMAL(10,5) UNSIGNED, PRIMARY KEY(id0, id1))",
			"INSERT INTO  decimal_table VALUES(-12345.12345,12345.12345,-12345.12345,12345.12345)",
			"INSERT INTO  decimal_table set id0=-67890.12345, id1=67890.12345",
		},

		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	db = "DB012"
	databases[db] = SchemaTextualColumns("char", db, 100)

	db = "DB013"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,

			// blobs in PK is not supported by RonDB
			"CREATE TABLE blob_table(id0 int, col0 blob, col1 int,  PRIMARY KEY(id0))",
			"INSERT INTO  blob_table VALUES(1,0xFFFF, 1)",
			"CREATE TABLE text_table(id0 int, col0 text, col1 int, PRIMARY KEY(id0))",
			"INSERT INTO  text_table VALUES(1,\"FFFF\", 1)",
		},

		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	db = "DB014" //varchar
	databases[db] = SchemaTextualColumns("VARCHAR", db, 50)

	db = "DB015" //long varchar
	databases[db] = SchemaTextualColumns("VARCHAR", db, 256)

	db = "DB016" //binary fix size
	databases[db] = SchemaTextualColumns("BINARY", db, 100)

	db = "DB017" //varbinary
	databases[db] = SchemaTextualColumns("VARBINARY", db, 100)

	db = "DB018" //long varbinary
	databases[db] = SchemaTextualColumns("VARBINARY", db, 256)

	db = "DB019"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,

			// blobs in PK is not supported by RonDB
			"CREATE TABLE `date_table` ( `id0`  date, `col0` date DEFAULT NULL, PRIMARY KEY (`id0`))",
			"insert into date_table values( \"1111-11-11\", \"11:11:11\")",
			"insert into date_table set id0= \"1111-11-12\" ",
		},

		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	db = "DB020"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,

			// blobs in PK is not supported by RonDB
			"CREATE TABLE `date_table0` ( `id0`  datetime(0), `col0` datetime(0) DEFAULT NULL, PRIMARY KEY (`id0`))",
			"insert into date_table0 values( \"1111-11-11 11:11:11\", \"1111-11-11 11:11:11\")",
			"insert into date_table0 set id0= \"1111-11-12 11:11:11\"",

			"CREATE TABLE `date_table3` ( `id0`  datetime(3), `col0` datetime(3) DEFAULT NULL, PRIMARY KEY (`id0`))",
			"insert into date_table3 values( \"1111-11-11 11:11:11.123\", \"1111-11-11 11:11:11.123\")",
			"insert into date_table3 set id0= \"1111-11-12 11:11:11.123\"",

			"CREATE TABLE `date_table6` ( `id0`  datetime(6), `col0` datetime(6) DEFAULT NULL, PRIMARY KEY (`id0`))",
			"insert into date_table6 values( \"1111-11-11 11:11:11.123456\", \"1111-11-11 11:11:11.123456\")",
			"insert into date_table6 set id0= \"1111-11-12 11:11:11.123456\"",
		},

		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	db = "DB021"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,

			// blobs in PK is not supported by RonDB
			"CREATE TABLE `time_table0` ( `id0`  time(0), `col0` time(0) DEFAULT NULL, PRIMARY KEY (`id0`))",
			"insert into time_table0 values( \"11:11:11\", \"11:11:11\")",
			"insert into time_table0 set id0= \"12:11:11\"",

			"CREATE TABLE `time_table3` ( `id0`  time(3), `col0` time(3) DEFAULT NULL, PRIMARY KEY (`id0`))",
			"insert into time_table3 values( \"11:11:11.123\", \"11:11:11.123\")",
			"insert into time_table3 set id0= \"12:11:11.123\"",

			"CREATE TABLE `time_table6` ( `id0` time(6), `col0` time(6) DEFAULT NULL, PRIMARY KEY (`id0`))",
			"insert into time_table6 values( \"11:11:11.123456\", \"11:11:11.123456\")",
			"insert into time_table6 set id0= \"12:11:11.123456\"",
		},

		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}

	db = "DB022"
	databases[db] = [][]string{
		{
			// setup commands
			"DROP DATABASE IF EXISTS " + db,
			"CREATE DATABASE " + db,
			"USE " + db,

			// blobs in PK is not supported by RonDB
			"CREATE TABLE `ts_table0` ( `id0`  timestamp(0), `col0` timestamp(0) DEFAULT NULL, PRIMARY KEY (`id0`))",
			"insert into ts_table0 values( \"2022-11-11 11:11:11\", \"2022-11-11 11:11:11\")",
			"insert into ts_table0 set id0= \"2022-11-12 11:11:11\"",

			"CREATE TABLE `ts_table3` ( `id0`  timestamp(3), `col0` timestamp(3) DEFAULT NULL, PRIMARY KEY (`id0`))",
			"insert into ts_table3 values( \"2022-11-11 11:11:11.123\", \"2022-11-11 11:11:11.123\")",
			"insert into ts_table3 set id0= \"2022-11-12 11:11:11.123\"",

			"CREATE TABLE `ts_table6` ( `id0`  timestamp(6), `col0` timestamp(6) DEFAULT NULL, PRIMARY KEY (`id0`))",
			"insert into ts_table6 values( \"2022-11-11 11:11:11.123456\", \"2022-11-11 11:11:11.123456\")",
			"insert into ts_table6 set id0= \"2022-11-12 11:11:11.123456\"",
		},

		{ // clean up commands
			"DROP DATABASE " + db,
		},
	}
}

func SchemaTextualColumns(colType string, db string, length int) [][]string {
	if strings.EqualFold(colType, "varbinary") || strings.EqualFold(colType, "binary") ||
		strings.EqualFold(colType, "char") || strings.EqualFold(colType, "varchar") {
		return [][]string{
			{
				// setup commands
				"DROP DATABASE IF EXISTS " + db,
				"CREATE DATABASE " + db,
				"USE " + db,

				// blobs in PK is not supported by RonDB
				"CREATE TABLE table1(id0 " + colType + "(" + strconv.Itoa(length) + "), col0 " + colType + "(" + strconv.Itoa(length) + "),  PRIMARY KEY(id0))",
				`INSERT INTO  table1 VALUES("1","这是一个测验。 我不知道怎么读中文。")`,
				`INSERT INTO  table1 VALUES("2",0x660066)`,
				`INSERT INTO  table1 VALUES("3","a\nb")`,
				`INSERT INTO  table1 VALUES("这是一个测验","12345")`,
				`INSERT INTO  table1 VALUES("4","ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïð")`, // some chars
				`INSERT INTO  table1 set id0=5`,
				`INSERT INTO  table1 VALUES("6","\"\\\b\f\n\r\t$%_?")`, // in mysql \f is replaced by f
			},

			{ // clean up commands
				"DROP DATABASE " + db,
			},
		}
	} else {
		panic("Data type not supported")
	}
}

func Database(name string) [][]string {
	db, ok := databases[name]
	if !ok {
		return [][]string{}
	}
	return db
}
