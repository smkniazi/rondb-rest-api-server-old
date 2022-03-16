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
	"hopsworks.ai/rdrs/internal/native"
	"hopsworks.ai/rdrs/internal/router/handler/batchops"
	"hopsworks.ai/rdrs/internal/router/handler/pkread"
	"hopsworks.ai/rdrs/internal/router/handler/stat"
)

type RouterConext struct {
	// REST Server
	Ip         string
	Port       int32
	APIVersion string
	Engine     *gin.Engine

	// RonDB
	ConnStr string
}

var _ Router = (*RouterConext)(nil)

func (rc *RouterConext) SetupRouter() error {
	rc.Engine = gin.Default()

	rc.Engine.GET("/"+rc.APIVersion+"/ping", stat.StatHandler)
	rc.Engine.POST("/"+rc.APIVersion+"/:db/:table/"+pkread.DB_OPERATION, pkread.PkReadHandler)
	rc.Engine.POST("/"+rc.APIVersion+"/"+batchops.DB_OPERATION, batchops.BatchOpsHandler)

	// connect to RonDB
	err := native.InitRonDBConnection(rc.ConnStr)
	if err != nil {
		return err
	}

	return nil
}

func (rc *RouterConext) StartRouter() error {

	address := fmt.Sprintf("%s:%d", rc.Ip, rc.Port)
	fmt.Printf("Listening on %s\n", address)
	err := rc.Engine.Run(address)
	if err != nil {
		return err
	}

	return nil
}

func CreateRouterContext() Router {
	router := RouterConext{
		Ip:         "localhost",
		Port:       8080,
		APIVersion: "1.0.0",
		ConnStr:    "localhost:1186",
	}
	return &router
}
