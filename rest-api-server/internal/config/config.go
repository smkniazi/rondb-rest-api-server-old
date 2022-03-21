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

package config

var ronDBConnString = "localhost:1186"

func RestAPIIP() string {
	return "localhost"
}

func RestAPIPort() int32 {
	return 8080
}

func RestAPIVersion() string {
	return "1.0.0"
}

func SetConnectionString(conStr string) {
	ronDBConnString = conStr
}

func ConnectionString() string {
	return ronDBConnString
}

func SqlUser() string {
	return "hop"
}

func SqlPassword() string {
	return "hop"
}

func SqlServerIP() string {
	return "localhost"
}

func SqlServerPort() int32 {
	return 3306
}
