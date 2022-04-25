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
	ds "hopsworks.ai/rdrs/internal/datastructs"
	tu "hopsworks.ai/rdrs/internal/router/handler/utils"
)

// func TestBatchSimple(t *testing.T) {

// testTable := "int_table"
// testDb := "DB004"
// validateColumns := []interface{}{"col0", "col1"}
// tests := map[string]ds.PKTestInfo{
// "simple1": {
// PkReq: ds.PKReadBody{Filters: tu.NewFiltersKVs(t, "id0", 0, "id1", 0),
// ReadColumns: tu.NewReadColumns(t, "col", 2),
// OperationID: tu.NewOperationID(t, 64),
// },
// Table:        testTable,
// Db:           testDb,
// HttpCode:     http.StatusOK,
// BodyContains: "",
// RespKVs:      validateColumns,
// },
// }

// tu.PkTest(t, tests, BatchOpsHandler, false)
// }

func TestBatchMissingReqField(t *testing.T) {
	router := gin.Default()
	group := router.Group(ds.DBS_OPS_EP_GROUP)
	group.POST(ds.BATCH_OPERATION, BatchOpsHandler)
	url := URL()

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

	// Test missing non-required fields
	operations = NewOperationsTBD(t, 1)
	*operations[0].Body = strings.Replace(*operations[0].Body, ds.READ_COL_PARAM_NAME, "XXX", -1)
	*operations[0].Body = strings.Replace(*operations[0].Body, ds.OPERATION_ID_PARAM_NAME, "XXX", -1)
	operationsWrapper = ds.Operations{Operations: &operations}
	body, _ = json.Marshal(operationsWrapper)
	tu.ProcessRequest(t, router, ds.BATCH_HTTP_VERB, url, string(body), http.StatusOK, "")
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

func URL() string {
	return fmt.Sprintf("%s%s", ds.DBS_OPS_EP_GROUP, ds.BATCH_OPERATION)
}
