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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"hopsworks.ai/rdrs/version"

	ds "hopsworks.ai/rdrs/internal/datastructs"
	tu "hopsworks.ai/rdrs/internal/router/handler/utils"
)

func TestPing(t *testing.T) {
	router := gin.Default()
	group := router.Group("/" + version.API_VERSION)
	group.GET(PATH, StatHandler)
	req, _ := http.NewRequest("GET", group.BasePath()+PATH, nil)
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)
	if resp.Code != 200 {
		t.Errorf("Test failed. Expected: %d, Got: %d", http.StatusOK, resp.Code)
	} else {
		t.Logf("Correct response received from the server")
	}
}

//func TestStat(t *testing.T) {
//	//int table DB004
//
//	tests := map[string]ds.BatchOperationTestInfo{
//		"date": { //single operation batch
//			HttpCode: http.StatusOK,
//			Operations: []ds.BatchSubOperationTestInfo{
//				createSubOperation(t, "int_table", "DB004", 0, 0, http.StatusOK),
//				// createSubOperation(t, "int_table", "DB004", 0, 0, http.StatusOK),
//				// createSubOperation(t, "int_table", "DB004", 0, 0, http.StatusOK),
//				// createSubOperation(t, "int_table", "DB004", 0, 0, http.StatusOK),
//				// createSubOperation(t, "int_table", "DB004", 0, 0, http.StatusOK),
//				// createSubOperation(t, "int_table", "DB004", 0, 0, http.StatusOK),
//				// createSubOperation(t, "int_table", "DB004", 0, 0, http.StatusOK),
//			},
//		},
//	}
//
//	tu.BatchTest(t, tests, false, batchops.RegisterBatchTestHandler, RegisterStatTestHandler)
//}

func createSubOperation(t *testing.T, table string, database string, pk1 int, pk2 int, expectedStatus int) ds.BatchSubOperationTestInfo {
	respKVs := []interface{}{"col0"}
	return ds.BatchSubOperationTestInfo{
		SubOperation: ds.BatchSubOperation{
			Method:      &[]string{ds.PK_HTTP_VERB}[0],
			RelativeURL: &[]string{string(database + "/" + table + "/" + ds.PK_DB_OPERATION)}[0],
			Body: &ds.PKReadBody{
				Filters:     tu.NewFiltersKVs(t, "id0", pk1, "id1", pk2),
				ReadColumns: tu.NewReadColumns(t, "col", 2),
				OperationID: tu.NewOperationID(t, 5),
			},
		},
		Table:        table,
		DB:           database,
		HttpCode:     expectedStatus,
		BodyContains: "",
		RespKVs:      respKVs,
	}
}
