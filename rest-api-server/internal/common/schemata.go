package common

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

var DB001 = [][]string{
	{
		// setup commands
		"CREATE DATABASE DB001",
		"USE DB001",
		"CREATE TABLE table_1(id0 VARCHAR(10), col_0 VARCHAR(100), col_1 VARCHAR(100), col_2 VARCHAR(100), PRIMARY KEY(id0))",
		"INSERT INTO table_1 VALUES('id0_data', 'col_0_data', 'col_1_data', 'col_2_data')",
	},

	{ // clean up commands
		"DROP DATABASE DB001",
	},
}

var DB002 = [][]string{
	{
		// setup commands
		"CREATE DATABASE DB002",
		"USE DB002",
		"CREATE TABLE table_1(id0 VARCHAR(10), id1 VARCHAR(10), col_0 VARCHAR(100), col_1 VARCHAR(100), col_2 VARCHAR(100), PRIMARY KEY(id0, id1))",
		"INSERT INTO table_1 VALUES('id0_data', 'id1_data', 'col_0_data', 'col_1_data', 'col_2_data')",
	},

	{ // clean up commands
		"DROP DATABASE DB002",
	},
}

var DB003 = [][]string{
	{
		// setup commands
		"CREATE DATABASE DB003",
		"USE DB003",
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
		"DROP DATABASE DB003",
	},
}

var DB004 = [][]string{
	{
		// signed and unsigned int
		// setup commands
		"CREATE DATABASE DB004",
		"USE DB004",
		"CREATE TABLE int_table(id0 INT, id1 INT UNSIGNED, col0 INT, col1 INT UNSIGNED, PRIMARY KEY(id0, id1))",
		"INSERT INTO int_table VALUES(2147483647,4294967295,2147483647,4294967295)",
		"INSERT INTO int_table VALUES(-2147483648,0,-2147483648,0)",
		"INSERT INTO int_table VALUES(1,1,1,1)",
	},

	{ // clean up commands
		"DROP DATABASE DB004",
	},
}
