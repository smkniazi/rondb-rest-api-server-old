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

package stat

import (
	"github.com/gin-gonic/gin"
	"hopsworks.ai/rdrs/internal/dal"
	ds "hopsworks.ai/rdrs/internal/datastructs"
	"hopsworks.ai/rdrs/version"
)

const PATH = "/stat"

func RegisterStatTestHandler(engine *gin.Engine) {
	engine.GET("/"+version.API_VERSION+"/"+ds.STAT_OPERATION, StatHandler)
}

func StatHandler(c *gin.Context) {
	var stats ds.StatInfo
	dal.GetRonDBStats(&stats)
	c.JSON(200, stats)
}
