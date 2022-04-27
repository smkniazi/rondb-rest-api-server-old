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
package batchops

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/gin-gonic/gin"
	"hopsworks.ai/rdrs/internal/common"
	"hopsworks.ai/rdrs/internal/dal"
	ds "hopsworks.ai/rdrs/internal/datastructs"
	"hopsworks.ai/rdrs/internal/router/handler/pkread"
	"hopsworks.ai/rdrs/version"
)

func RegisterBatchTestHandler(engine *gin.Engine) {
	engine.POST("/"+version.API_VERSION+"/"+ds.BATCH_OPERATION, BatchOpsHandler)
}

func BatchOpsHandler(c *gin.Context) {
	operations := ds.BatchOperation{}
	err := c.ShouldBindJSON(&operations)
	if err != nil {
		fmt.Printf("Unable to parse request. Error: %v\n", err)
		c.JSON(http.StatusBadRequest, gin.H{"OK": false, "msg": fmt.Sprintf("%-v", err)})
		return
	}

	if operations.Operations == nil {
		c.JSON(http.StatusBadRequest, gin.H{"OK": false, "msg": "No valid operations found"})
		return
	}

	pkOperations := make([]ds.PKReadParams, len(*operations.Operations))
	for i, operation := range *operations.Operations {
		err := parseOperation(&operation, &pkOperations[i])
		if err != nil {
			fmt.Printf("Error: %v", err)
			c.JSON(http.StatusBadRequest, gin.H{"OK": false, "msg": fmt.Sprintf("%-v", err)})
			return
		}
	}

	noOps := uint32(len(pkOperations))
	reqPtrs := make([]*dal.Native_Buffer, noOps)
	respPtrs := make([]*dal.Native_Buffer, noOps)

	for i, pkOp := range pkOperations {
		b, _ := json.Marshal(pkOp)

		fmt.Printf("%s\n", string(b))
		reqPtrs[i], respPtrs[i], err = pkread.CreateNativeRequest(&pkOp)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"OK": false, "msg": fmt.Sprintf("%v", err)})
			return
		}
	}

	dalErr := dal.RonDBBatchedPKRead(noOps, reqPtrs, respPtrs)

	var message string
	if dalErr != nil {
		if dalErr.HttpCode >= http.StatusInternalServerError {
			message = fmt.Sprintf("%v File: %v, Line: %v ", dalErr.Message, dalErr.ErrFileName, dalErr.ErrLineNo)
		} else {
			message = fmt.Sprintf("%v", dalErr.Message)
		}
		setResponseError(c, dalErr.HttpCode, common.ErrorResponse{Error: message})
	} else {

		c.Writer.Write(([]byte)(string("[")))
		for i := uint32(0); i < noOps; i++ {
			setResponseBodyUnsafe(c, http.StatusOK, respPtrs[i], i != (noOps-1))
		}
		c.Writer.Write(([]byte)(string("]")))
	}
}

func setResponseError(c *gin.Context, code int, resp common.ErrorResponse) {
	b, _ := json.Marshal(resp) // only used in case of errors so not terrible for performance
	c.String(code, string(b))
}

func setResponseBodyUnsafe(c *gin.Context, code int, resp *dal.Native_Buffer, appendComma bool) {
	c.Writer.WriteHeader(code)
	c.Writer.Write(([]byte)(common.ProcessResponse(resp.Buffer)))
	if appendComma {
		c.Writer.Write(([]byte)(string(",")))
	}
}

func parseOperation(operation *ds.BatchSubOperation, pkReadarams *ds.PKReadParams) error {

	//remove leading / character
	if strings.HasPrefix(*operation.RelativeURL, "/") {
		trimmed := strings.Trim(*operation.RelativeURL, "/")
		operation.RelativeURL = &trimmed
	}

	match, err := regexp.MatchString("^[a-zA-Z0-9$_]+/[a-zA-Z0-9$_]+/pk-read",
		*operation.RelativeURL)
	if !match || err != nil {
		return fmt.Errorf("Invalid Relative URL: %s", *operation.RelativeURL)
	} else {
		err := parsePKRead(operation, pkReadarams)
		if err != nil {
			return err
		}
	}
	return nil
}

func parsePKRead(operation *ds.BatchSubOperation, pkReadarams *ds.PKReadParams) error {
	params := *operation.Body

	//split the relative url to extract path parameters
	splits := strings.Split(*operation.RelativeURL, "/")
	if len(splits) != 3 {
		return fmt.Errorf("Failed to extract database and table information from relative url")
	}

	pkReadarams.DB = &splits[0]
	pkReadarams.Table = &splits[1]
	pkReadarams.Filters = params.Filters
	pkReadarams.ReadColumns = params.ReadColumns
	pkReadarams.OperationID = params.OperationID
	return nil
}
