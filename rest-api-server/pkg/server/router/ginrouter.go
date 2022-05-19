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

package router

import (
	"fmt"

	"github.com/gin-gonic/gin"
	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/dal"
	ds "hopsworks.ai/rdrs/internal/datastructs"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/internal/router/handler/batchops"
	"hopsworks.ai/rdrs/internal/router/handler/pkread"
	"hopsworks.ai/rdrs/internal/router/handler/stat"
	// _ "github.com/ianlancetaylor/cgosymbolizer" // enable this for stack trace for c layer
)

type RouterConext struct {
	// REST Server
	ServerIP   string
	ServerPort uint16
	APIVersion string
	Engine     *gin.Engine

	// RonDB
	DBIP   string
	DBPort uint16
}

var _ Router = (*RouterConext)(nil)

func (rc *RouterConext) SetupRouter() error {
	gin.SetMode(gin.ReleaseMode)
	rc.Engine = gin.New()

	rc.Engine.GET("/"+rc.APIVersion+"/"+ds.STAT_OPERATION, stat.StatHandler)
	rc.Engine.POST("/"+rc.APIVersion+"/:db/:table/"+ds.PK_DB_OPERATION, pkread.PkReadHandler)
	rc.Engine.POST("/"+rc.APIVersion+"/"+ds.BATCH_OPERATION, batchops.BatchOpsHandler)

	// connect to RonDB
	dal.InitializeBuffers()
	err := dal.InitRonDBConnection(fmt.Sprintf("%s:%d", rc.DBIP, rc.DBPort), false)
	if err != nil {
		return err
	}

	return nil
}

func (rc *RouterConext) StartRouter() error {

	address := fmt.Sprintf("%s:%d", rc.ServerIP, rc.ServerPort)
	log.Infof("Listening on %s\n", address)
	err := rc.Engine.Run(address)
	if err != nil {
		return err
	}

	return nil
}

func CreateRouterContext() Router {
	router := RouterConext{
		ServerIP:   config.Configuration().RestServer.IP,
		ServerPort: config.Configuration().RestServer.Port,
		APIVersion: config.Configuration().RestServer.APIVersion,
		DBIP:       config.Configuration().RonDBConfig.IP,
		DBPort:     config.Configuration().RonDBConfig.Port,
	}
	return &router
}
