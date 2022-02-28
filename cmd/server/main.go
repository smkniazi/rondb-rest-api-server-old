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

package main

import (
	"fmt"
	"log"

	"hopsworks.ai/rondb-rest-api-server/pkg/server/router"
	"hopsworks.ai/rondb-rest-api-server/version"
)

func main() {
	log.Printf("Starting Version : %s, Git Branch: %s (%s). Built on %s at %s  \n",
		version.VERSION, version.BRANCH, version.GITCOMMIT, version.BUILDTIME, version.HOSTNAME)
	log.Printf("Starting API Version : %s  \n", version.API_VERSION)
	router := router.CreateRouterContext()
	router.SetupRouter()
	router.StartRouter()
	fmt.Println("Bye ...")
}
