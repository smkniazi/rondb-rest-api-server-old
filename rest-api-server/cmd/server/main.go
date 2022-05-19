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
	"flag"
	"fmt"
	"runtime"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/pkg/server/router"
	"hopsworks.ai/rdrs/version"
)

func main() {
	configFile := flag.String("config", "", "Configuration file path")
	logFile := flag.String("logFile", "", "Log file path. By default the log is written to console")
	logLevel := flag.String("logLevel", "info", "Levels: error, warn, info, debug, trace")
	flag.Parse()

	if *configFile != "" {
		config.LoadConfig(*configFile, true)
	}

	log.InitLogger(*logLevel, *logFile)

	log.Infof("Starting Version : %s, Git Branch: %s (%s). Built on %s at %s  \n",
		version.VERSION, version.BRANCH, version.GITCOMMIT, version.BUILDTIME, version.HOSTNAME)
	log.Infof("Starting API Version : %s  \n", version.API_VERSION)

	runtime.GOMAXPROCS(config.Configuration().RestServer.GOMAXPROCS)

	router := router.CreateRouterContext()
	err := router.SetupRouter()
	if err != nil {
		log.Panic(fmt.Sprintf("Unable to setup router: Error: %v", err))
	}
	err = router.StartRouter()
	if err != nil {
		log.Panic(fmt.Sprintf("Unable to start router: Error: %v", err))
	}

	log.Info("Shutting down REST server")
}
