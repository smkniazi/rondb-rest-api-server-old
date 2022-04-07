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
	"unsafe"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"hopsworks.ai/rdrs/internal/common"
	"hopsworks.ai/rdrs/internal/dal"
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
	DB          *string       `json:"db" `
	Table       *string       `json:"table"`
	Filters     *[]Filter     `json:"filters"`
	ReadColumns *[]ReadColumn `json:"readColumns"`
	OperationID *string       `json:"operationId"`
}

// Path parameters
type PKReadPP struct {
	DB    *string `json:"db" uri:"db"  binding:"required,min=1,max=64"`
	Table *string `json:"table" uri:"table"  binding:"required,min=1,max=64"`
}

type PKReadBody struct {
	Filters     *[]Filter     `json:"filters"         form:"filters"         binding:"required,min=1,max=4096,dive"`
	ReadColumns *[]ReadColumn `json:"readColumns"    form:"read-columns"    binding:"omitempty,min=1,max=4096,unique"`
	OperationID *string       `json:"operationId"    form:"operation-id"    binding:"omitempty,min=1,max=64"`
}

type Filter struct {
	Column *string `json:"column"   form:"column"   binding:"required,min=1,max=64"`
	Value  *string `json:"value"    form:"value"    binding:"required"`
}

const (
	DRT_DEFAULT = "default"
	DRT_BASE64  = "base64" // not implemented yet
	DRT_HEX     = "hex"    // not implemented yet
)

type ReadColumn struct {
	Column *string `json:"column"    form:"column"    binding:"required,min=1,max=64"`

	// Data return type you can change the return type for the column data
	// int/floats/decimal are returned as JSON Number type (default),
	// varchar/char are returned as strings (default) and varbinary as base64 (default)
	// Right now only default return type is supported
	DataReturnType *string `json:"dataReturnType"    form:"column"    binding:"Enum=default,min=1,max=64"`

	// more parameter can be added later.
}

func PkReadHandler(c *gin.Context) {

	pkReadParams := PKReadParams{}

	err := parseRequest(c, &pkReadParams)
	if err != nil {
		fmt.Printf("Unable to parse request. Error: %v", err)
		c.AbortWithError(http.StatusBadRequest, err)
		setResponseError(c, http.StatusBadRequest, common.Response{OK: false, Message: fmt.Sprintf("%-v", err)})
		return
	}

	request, response, err := createNativeRequest(&pkReadParams)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"OK": false, "msg": fmt.Sprintf("%v", err)})
		return
	}

	dalErr := dal.RonDBPKRead(request, response)
	var message string
	if dalErr != nil {
		if dalErr.HttpCode >= http.StatusInternalServerError {
			message = fmt.Sprintf("%v File: %v, Line: %v ", dalErr.Message, dalErr.ErrFileName, dalErr.ErrLineNo)
		} else {
			message = fmt.Sprintf("%v", dalErr.Message)
		}
		setResponseError(c, dalErr.HttpCode, common.Response{OK: false, Message: message})
	} else {
		setResponseBodyUnsafe(c, http.StatusOK, response)
	}
}

func setResponseError(c *gin.Context, code int, resp common.Response) {
	b, _ := json.Marshal(resp) // only used in case of errors so not terrible for performance
	c.String(code, string(b))
}

func setResponseBodyUnsafe(c *gin.Context, code int, resp unsafe.Pointer) {
	res := common.Response{OK: true, Message: common.ProcessResponse(resp)} // TODO XXX Fix this. Use response writer. Benchmark this part
	b, _ := json.Marshal(res)
	c.String(code, string(b))
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
	existingFilters := make(map[string]bool)
	for _, filter := range *params.Filters {
		if _, value := existingFilters[*filter.Column]; value {
			return fmt.Errorf("field validation for filter failed on the 'unique' tag")
		} else {
			existingFilters[*filter.Column] = true
		}
	}

	// make sure read columns are valid
	if params.ReadColumns != nil {
		for _, col := range *params.ReadColumns {
			if err := validateDBIdentifier(*col.Column); err != nil {
				return err
			}
		}
	}

	// make sure that the filter columns and read colummns do not overlap
	// and read cols are unique
	if params.ReadColumns != nil {
		existingCols := make(map[string]bool)
		for _, readCol := range *params.ReadColumns {
			if _, value := existingFilters[*readCol.Column]; value {
				return fmt.Errorf("field validation for read columns faild. '%s' already included in filter", *readCol.Column)
			}

			if _, value := existingCols[*readCol.Column]; value {
				return fmt.Errorf("field validation for 'ReadColumns' failed on the 'unique' tag.")
			} else {
				existingCols[*readCol.Column] = true
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

	//https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
	for _, r := range identifier {
		if !((r >= rune(0x0001) && r <= rune(0x007F)) || (r >= rune(0x0080) && r <= rune(0x0FFF))) {
			return fmt.Errorf("field validation failed. Invalid character '%U' ", r)
		}
	}
	return nil
}
