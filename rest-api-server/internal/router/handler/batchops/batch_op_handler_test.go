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
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"hopsworks.ai/rdrs/internal/common"
	ds "hopsworks.ai/rdrs/internal/datastructs"
	tu "hopsworks.ai/rdrs/internal/router/handler/utils"
	"hopsworks.ai/rdrs/version"
)

func TestBatchSimple(t *testing.T) {

	//int table DB004
	pkMethod := ds.PK_HTTP_VERB
	pkr0 := ds.PKReadBody{Filters: tu.NewFiltersKVs(t, "id0", 0, "id1", 0),
		ReadColumns: tu.NewReadColumns(t, "col", 2),
		OperationID: tu.NewOperationID(t, 64),
	}
	rurl0 := version.API_VERSION + "/DB004/int_table/" + ds.PK_DB_OPERATION

	// Bigint  table "DB005"
	pkr1 := ds.PKReadBody{Filters: tu.NewFiltersKVs(t, "id0", 0, "id1", 0),
		ReadColumns: tu.NewReadColumns(t, "col", 2),
		OperationID: tu.NewOperationID(t, 64),
	}
	rurl1 := version.API_VERSION + "/DB005/bigint_table/" + ds.PK_DB_OPERATION

	operations := make([]ds.Operation, 2)
	operations[0].Method = &pkMethod
	operations[0].RelativeURL = &rurl0
	opBody, _ := json.MarshalIndent(pkr0, "", "\t")
	opBodyStr := string(opBody)
	operations[0].Body = &opBodyStr

	operations[1].Method = &pkMethod
	operations[1].RelativeURL = &rurl1
	opBody, _ = json.MarshalIndent(pkr1, "", "\t")
	opBodyStr = string(opBody)
	operations[1].Body = &opBodyStr

	batchOperation := ds.Operations{Operations: &operations}
	batchOperationJson, _ := json.MarshalIndent(batchOperation, "", "\t")
	batchOperationJsonStr := string(batchOperationJson)

	router := gin.Default()
	group := router.Group(ds.DBS_OPS_EP_GROUP)
	group.POST(ds.BATCH_OPERATION, BatchOpsHandler)
	url := BatchURL()

	tu.WithDBs(t, [][][]string{common.Database("DB004"), common.Database("DB005")}, RegisterBatchTestHandler, func(router *gin.Engine) {
		tu.ProcessRequest(t, router, ds.BATCH_HTTP_VERB, url, batchOperationJsonStr, http.StatusOK, "")
	})
}

func TestBatchMissingReqField(t *testing.T) {
	router := gin.Default()
	group := router.Group(ds.DBS_OPS_EP_GROUP)
	group.POST(ds.BATCH_OPERATION, BatchOpsHandler)
	url := BatchURL()

	// Test missing method
	operations := NewOperationsTBD(t, 3)
	operations[1].Method = nil
	operationsWrapper := ds.Operations{Operations: &operations}
	body, _ := json.Marshal(operationsWrapper)
	tu.ProcessRequest(t, router, ds.BATCH_HTTP_VERB, url, string(body), http.StatusBadRequest,
		"Error:Field validation for 'Method' failed ")

	// Test missing relative URL
	operations = NewOperationsTBD(t, 3)
	operations[1].RelativeURL = nil
	operationsWrapper = ds.Operations{Operations: &operations}
	body, _ = json.Marshal(operationsWrapper)
	tu.ProcessRequest(t, router, ds.BATCH_HTTP_VERB, url, string(body), http.StatusBadRequest,
		"Error:Field validation for 'RelativeURL' failed ")

	// Test missing body
	operations = NewOperationsTBD(t, 3)
	operations[1].Body = nil
	operationsWrapper = ds.Operations{Operations: &operations}
	body, _ = json.Marshal(operationsWrapper)
	tu.ProcessRequest(t, router, ds.BATCH_HTTP_VERB, url, string(body), http.StatusBadRequest,
		"Error:Field validation for 'Body' failed ")

	// Test missing filter in an operation
	operations = NewOperationsTBD(t, 3)
	*operations[1].Body = strings.Replace(*operations[1].Body, ds.FILTER_PARAM_NAME, "XXX", -1)
	operationsWrapper = ds.Operations{Operations: &operations}
	body, _ = json.Marshal(operationsWrapper)
	tu.ProcessRequest(t, router, ds.BATCH_HTTP_VERB, url, string(body), http.StatusBadRequest,
		"Error:Field validation for 'Filters' failed")
}

func NewOperationsTBD(t *testing.T, numOps int) []ds.Operation {
	operations := make([]ds.Operation, numOps)
	for i := 0; i < numOps; i++ {
		operations[i] = NewOperationTBD(t)
	}
	return operations
}

func NewOperationTBD(t *testing.T) ds.Operation {
	opBody, _ := json.MarshalIndent(tu.NewPKReadReqBodyTBD(t), "", "\t")
	opBodyStr := string(opBody)
	method := "POST"
	relativeURL := tu.NewPKReadURL("db", "table")

	op := ds.Operation{
		Method:      &method,
		RelativeURL: &relativeURL,
		Body:        &opBodyStr,
	}

	return op
}

func BatchURL() string {
	return fmt.Sprintf("%s%s", ds.DBS_OPS_EP_GROUP, ds.BATCH_OPERATION)
}
