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
package utils

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func ProcessRequest(t *testing.T, router *gin.Engine, httpVerb string,
	url string, body string, expectedStatus int, expectedMsg string) {
	req, _ := http.NewRequest(httpVerb, url, strings.NewReader(body))
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	if resp.Code != expectedStatus || !strings.Contains(resp.Body.String(), expectedMsg) {
		if resp.Code != expectedStatus {
			t.Errorf("Test failed. Expected: %d, Got: %d. ", expectedStatus, resp.Code)
		}
		if !strings.Contains(resp.Body.String(), expectedMsg) {
			t.Errorf("Test failed. Response body does not contain %s. Body: %s", expectedMsg, resp.Body)
		}
	}
}
