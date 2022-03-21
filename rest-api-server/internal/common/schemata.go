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
