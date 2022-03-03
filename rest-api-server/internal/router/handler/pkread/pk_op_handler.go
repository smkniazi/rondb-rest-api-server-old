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

package pkread

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"hopsworks.ai/rdrs/version"
)

const DB_PP = "db"
const TABLE_PP = "table"
const DB_OPS_EP_GROUP = "/" + version.API_VERSION + "/:" + DB_PP + "/:" + TABLE_PP + "/"
const DB_OPERATION = "pk-read"
const HTTP_VERB = "POST"

// Primary key column filter
const FILTER_PARAM_NAME = "filters"
const READ_COL_PARAM_NAME = "read-columns"
const OPERATION_ID_PARAM_NAME = "operation-id"

type PKReadParams struct {
	DB          *string   `json:"db" `
	Table       *string   `json:"table"`
	Filters     *[]Filter `json:"filters"`
	ReadColumns *[]string `json:"read-columns"`
	OperationID *string   `json:"operation-id"`
}

// Path parameters
type PKReadPP struct {
	DB    *string `json:"db" uri:"db"  binding:"required,min=1,max=64"`
	Table *string `json:"table" uri:"table"  binding:"required,min=1,max=64"`
}

type PKReadBody struct {
	Filters     *[]Filter `json:"filters"         form:"filters"         binding:"required,min=1,max=4096,dive"`
	ReadColumns *[]string `json:"read-columns"    form:"read-columns"    binding:"omitempty,min=1,max=4096,unique"`
	OperationID *string   `json:"operation-id"    form:"operation-id"    binding:"omitempty,min=1,max=64"`
}

type Filter struct {
	Column *string `json:"column"   form:"column"   binding:"required,min=1,max=64"`
	Value  *string `json:"value"    form:"value"    binding:"required"`
}

func PkReadHandler(c *gin.Context) {

	pkReadParams := PKReadParams{}

	err := parseRequest(c, &pkReadParams)
	if err != nil {
		fmt.Printf("Unable to parse request. Error: %v", err)
		c.AbortWithError(http.StatusBadRequest, err)
		c.JSON(http.StatusBadRequest, gin.H{"OK": false, "msg": fmt.Sprintf("%-v", err)})
		return
	}

	fmt.Printf("Full URI: %s\n", c.Request.URL)
	msg, _ := json.MarshalIndent(pkReadParams, "", "\t")
	fmt.Printf("Request Params: %s\n", msg)
	c.JSON(http.StatusOK, gin.H{"OK": true, "msg": "All Good"})
}

func parseRequest(c *gin.Context, pkReadParams *PKReadParams) error {

	body := PKReadBody{}
	pp := PKReadPP{}

	if err := parseURI(c, &pp); err != nil {
		return err
	}

	if err := ParseBody(c.Request, &body); err != nil {
		return err
	}

	pkReadParams.DB = pp.DB
	pkReadParams.Table = pp.Table
	pkReadParams.Filters = body.Filters
	pkReadParams.ReadColumns = body.ReadColumns
	pkReadParams.OperationID = body.OperationID
	return nil
}

func ParseBody(req *http.Request, params *PKReadBody) error {

	b := binding.JSON
	err := b.Bind(req, &params)
	if err != nil {
		return err
	}

	// make sure filter columns are valid
	for _, filter := range *params.Filters {
		if err := validateDBIdentifier(*filter.Column); err != nil {
			return err
		}
	}

	// make sure that the columns are unique.
	exists := make(map[string]bool)
	for _, filter := range *params.Filters {
		if _, value := exists[*filter.Column]; value {
			return fmt.Errorf("field validation for filter failed on the 'unique' tag")
		} else {
			exists[*filter.Column] = true
		}
	}

	// make sure read columns are valid
	if params.ReadColumns != nil {
		for _, col := range *params.ReadColumns {
			if err := validateDBIdentifier(col); err != nil {
				return err
			}
		}
	}

	// make sure that the filter columns and read colummns do not overlap
	if params.ReadColumns != nil {
		exists = make(map[string]bool)
		for _, filter := range *params.Filters {
			exists[*filter.Column] = true
		}
		for _, readCol := range *params.ReadColumns {
			if _, value := exists[readCol]; value {
				return fmt.Errorf("field validation for read columns faild. '%s' already included in filter", readCol)
			}
		}
	}

	return nil
}

func parseURI(c *gin.Context, resource *PKReadPP) error {
	err := c.ShouldBindUri(&resource)
	if err != nil {
		return err
	}

	if err = validateDBIdentifier(*resource.DB); err != nil {
		return err
	}

	if err = validateDBIdentifier(*resource.Table); err != nil {
		return err
	}

	return nil
}

func validateDBIdentifier(identifier string) error {
	if len(identifier) < 1 || len(identifier) > 64 {
		return fmt.Errorf("field length validation failed")
	}

	for _, r := range identifier {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || (r == '_') || r == '$') {
			return fmt.Errorf("field validation failed. Invalid character '%c' ", r)
		}
	}
	return nil
}
