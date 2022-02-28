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
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"hopsworks.ai/rondb-rest-api-server/version"
)

const DB_PP = "db"
const TABLE_PP = "table"
const DB_OPS_EP_GROUP = "/" + version.API_VERSION + "/:" + DB_PP + "/:" + TABLE_PP + "/"
const DB_OPERATION = "pk-read"

// Primary key column filter
const FILTER_PARAM_NAME = "filter"
const READ_COL_PARAM_NAME = "read-column"
const OPERATION_ID_PARAM_NAME = "operation-id"

type Resourse struct {
	DB    *string `json:"db" uri:"db"  binding:"required,min=1,max=64"`
	Table *string `json:"table" uri:"table"  binding:"required,min=1,max=64"`
}

type PKReadParams struct {
	Filters     *[]Filter `json:"filter"         form:"filter"          binding:"required,min=1,max=4096,dive"`
	ReadColumns *[]string `json:"read-column"    form:"read-column"     binding:"omitempty,min=1,max=4096,unique"`
	OperationID *string   `json:"operation-id"   form:"operation-id"    binding:"omitempty,min=1,max=64"`
}

type Filter struct {
	Column *string `json:"column"   form:"column"   binding:"required,min=1,max=64"`
	Value  *string `json:"value"    form:"value"    binding:"required"`
}

func PkReadHandler(c *gin.Context) {

	params := PKReadParams{}
	resource := Resourse{}

	err := parseRequest(c, &resource, &params)
	if err != nil {
		fmt.Printf("Unable to parse request. Error: %v", err)
		c.AbortWithError(http.StatusBadRequest, err)
		c.JSON(http.StatusBadRequest, gin.H{"OK": false, "msg": fmt.Sprintf("%-v", err)})
		return
	}

	fmt.Printf("Full URI: %s\n", c.Request.URL)
	fmt.Printf("DB: %s, Table: %s\n", *resource.DB, *resource.Table)
	fmt.Printf("Filter: %-v\n", params.Filters)
	if params.ReadColumns != nil {
		fmt.Printf("Read Columns: %s\n", params.ReadColumns)
	}

	if params.OperationID != nil {
		fmt.Printf("Operation ID: %s\n", *params.OperationID)
	}
	c.JSON(http.StatusOK, gin.H{"OK": true, "msg": "All Good"})
}

func parseRequest(c *gin.Context, resource *Resourse, params *PKReadParams) error {

	if err := parseURI(c, resource); err != nil {
		return err
	}

	if err := parseQuery(c, params); err != nil {
		return err
	}

	return nil
}

func parseQuery(c *gin.Context, params *PKReadParams) error {
	err := c.ShouldBindQuery(&params)
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
			return fmt.Errorf("Field validation for filter failed on the 'unique' tag")
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
				return fmt.Errorf("Field validation for read columns faild. '%s' already included in filter", readCol)
			}
		}
	}

	return nil
}

func parseURI(c *gin.Context, resource *Resourse) error {
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
		return fmt.Errorf("Field length validation failed")
	}

	for _, r := range identifier {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || (r == '_') || r == '$') {
			return fmt.Errorf("Field validation failed. Invalid character '%c' ", r)
		}
	}
	return nil
}
