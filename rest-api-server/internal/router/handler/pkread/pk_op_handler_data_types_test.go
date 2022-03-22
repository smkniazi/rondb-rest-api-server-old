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
	"testing"

	"github.com/gin-gonic/gin"
	"hopsworks.ai/rdrs/internal/common"
	tu "hopsworks.ai/rdrs/internal/router/handler/utils"
)

// Test signed and unsigned int data type
func TestIntDataType(t *testing.T) {
	withDBs(t, [][][]string{common.DB004}, func(router *gin.Engine) {
		url := NewPKReadURL("DB004", "int_table")

		// simple
		param := PKReadBody{
			Filters:     NewFiltersKVs(t, "id0", "1", "id1", "1"),
			ReadColumns: NewReadColumns(t, "col", 2),
			OperationID: NewOperationID(t, 64),
		}
		body, _ := json.MarshalIndent(param, "", "\t")
		res := tu.ProcessRequest(t, router, HTTP_VERB, url, string(body), http.StatusOK, "")
		fmt.Printf("Response %v\n", res)
		tu.ValidateResponse(t, res, "col0", "1", "col1", "1")

		// max vlaues
		param = PKReadBody{
			Filters:     NewFiltersKVs(t, "id0", "2147483647", "id1", "4294967295"),
			ReadColumns: NewReadColumns(t, "col", 2),
		}
		body, _ = json.MarshalIndent(param, "", "\t")
		res = tu.ProcessRequest(t, router, HTTP_VERB, url, string(body), http.StatusOK, "")
		fmt.Printf("Response %v\n", res)
		tu.ValidateResponse(t, res, "col0", "2147483647", "col1", "4294967295")

		//min values
		param = PKReadBody{
			Filters:     NewFiltersKVs(t, "id0", "-2147483648", "id1", "0"),
			ReadColumns: NewReadColumns(t, "col", 2),
		}
		body, _ = json.MarshalIndent(param, "", "\t")
		res = tu.ProcessRequest(t, router, HTTP_VERB, url, string(body), http.StatusOK, "")
		fmt.Printf("Response %v\n", res)
		tu.ValidateResponse(t, res, "col0", "-2147483648", "col1", "0")
	})
}

func TestIntDataTypeErrors(t *testing.T) {
	withDBs(t, [][][]string{common.DB004}, func(router *gin.Engine) {
		url := NewPKReadURL("DB004", "int_table")

		// assigning signed value to unsigned column
		param := PKReadBody{
			Filters:     NewFiltersKVs(t, "id0", "1", "id1", "-1"), //id1 is unsigned
			ReadColumns: NewReadColumns(t, "col", 2),
			OperationID: NewOperationID(t, 64),
		}
		body, _ := json.MarshalIndent(param, "", "\t")
		tu.ProcessRequest(t, router, HTTP_VERB, url, string(body), http.StatusBadRequest, common.ERROR_015())

		// assigning bigger values
		param = PKReadBody{
			Filters:     NewFiltersKVs(t, "id0", "2147483648", "id1", "4294967295"),
			ReadColumns: NewReadColumns(t, "col", 2),
		}
		body, _ = json.MarshalIndent(param, "", "\t")
		tu.ProcessRequest(t, router, HTTP_VERB, url, string(body), http.StatusBadRequest, common.ERROR_015())

		// assigning smaller values
		param = PKReadBody{
			Filters:     NewFiltersKVs(t, "id0", "-2147483649", "id1", "0"),
			ReadColumns: NewReadColumns(t, "col", 2),
		}
		body, _ = json.MarshalIndent(param, "", "\t")
		tu.ProcessRequest(t, router, HTTP_VERB, url, string(body), http.StatusBadRequest, common.ERROR_015())

	})
}
