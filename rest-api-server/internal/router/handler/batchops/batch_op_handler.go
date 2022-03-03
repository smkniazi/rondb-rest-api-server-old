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
	"github.com/gin-gonic/gin/binding"
	"hopsworks.ai/rdrs/internal/router/handler/pkread"
	"hopsworks.ai/rdrs/version"
)

const DB_OPS_EP_GROUP = "/" + version.API_VERSION + "/"
const DB_OPERATION = "batch"
const HTTP_VERB = "POST"

type Operations struct {
	Operations *[]Operation `json:"operations" binding:"required,min=1,max=4096,unique,dive"`
}

type Operation struct {
	Method      *string `json:"method"        binding:"required,oneof=POST"`
	RelativeURL *string `json:"relative-url"  binding:"required,min=1"`
	Body        *string `json:"body"          binding:"required,min=1"`
}

func BatchOpsHandler(c *gin.Context) {
	operations := Operations{}
	err := c.ShouldBindJSON(&operations)
	if err != nil {
		fmt.Printf("Unable to parse request. Error: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"OK": false, "msg": fmt.Sprintf("%-v", err)})
		return
	}

	if operations.Operations == nil {
		c.JSON(http.StatusBadRequest, gin.H{"OK": false, "msg": "No valid operations found"})
		return
	}

	pkOperations := make([]pkread.PKReadParams, len(*operations.Operations))
	for i, operation := range *operations.Operations {
		err := parseOperation(&operation, &pkOperations[i])
		if err != nil {
			fmt.Printf("Error: %v", err)
			c.JSON(http.StatusBadRequest, gin.H{"OK": false, "msg": fmt.Sprintf("%-v", err)})
			return
		}
	}

	for _, pkOps := range pkOperations {
		msg, _ := json.MarshalIndent(pkOps, "", "\t")
		fmt.Printf("Operation Received %s", msg)
	}

	c.JSON(http.StatusOK, gin.H{"OK": true, "msg": "All Good"})
}

func parseOperation(operation *Operation, pkReadarams *pkread.PKReadParams) error {

	//remove leading / character
	if strings.HasPrefix(*operation.RelativeURL, "/") {
		trimmed := strings.Trim(*operation.RelativeURL, "/")
		operation.RelativeURL = &trimmed
	}

	match, err := regexp.MatchString("^[0-9].[0-9].[0-9]/[a-zA-Z0-9$_]*/[a-zA-Z0-9$_]*/pk-read",
		*operation.RelativeURL)
	if !match || err != nil {
		return fmt.Errorf("Invalid Relative URL: %s", *operation.RelativeURL)
	} else {
		err := parsePKRead(operation, pkReadarams)
		if err != nil {
			return err
		} else {
		}
	}
	return nil
}

func parsePKRead(operation *Operation, pkReadarams *pkread.PKReadParams) error {
	req, err := http.NewRequest(*operation.Method, *operation.RelativeURL,
		strings.NewReader(*operation.Body))
	if err != nil {
		return err
	}

	b := binding.JSON
	params := pkread.PKReadBody{}
	err = b.Bind(req, &params)
	if err != nil {
		return err
	}

	//split the relative url to extract path parameters
	splits := strings.Split(*operation.RelativeURL, "/")
	fmt.Printf(" Splits: %-v", splits)
	if len(splits) != 4 {
		return fmt.Errorf("Failed to extract database and table information from relative url")
	}

	pkReadarams.DB = &splits[1]
	pkReadarams.Table = &splits[2]
	pkReadarams.Filters = params.Filters
	pkReadarams.ReadColumns = params.ReadColumns
	pkReadarams.OperationID = params.OperationID
	return nil
}
