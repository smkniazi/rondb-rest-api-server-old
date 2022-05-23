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
	"time"

	"github.com/gin-gonic/gin"
	"hopsworks.ai/rdrs/internal/common"
	ds "hopsworks.ai/rdrs/internal/datastructs"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/internal/router/handler/stat"
	tu "hopsworks.ai/rdrs/internal/router/handler/utils"
)

// go test  -test.bench BenchmarkSimple -test.run=thisexpressionwontmatchanytest  ./internal/router/handler/pkread/
// -benchtime=100x // 100 times
// -benchtime=10s // 10 sec
//-cpu 1,2,4,8 -benchmem
func BenchmarkSimple(t *testing.B) {

	log.Infof("-------- b.N: %d ---------\n", t.N)
	db := "bench"
	table := "table_1"
	maxRows := 1000
	opCount := 0
	threadId := 0
	tu.WithDBs(t, [][][]string{common.Database(db)},
		[]tu.RegisterTestHandler{RegisterPKTestHandler, stat.RegisterStatTestHandler}, func(router *gin.Engine) {

			t.ResetTimer()
			start := time.Now()

			t.RunParallel(func(bp *testing.PB) {

				url := tu.NewPKReadURL(db, table)
				operationId := fmt.Sprintf("operation_%d", threadId)
				threadId++

				opCount++
				reqBody := createReq(maxRows, opCount, operationId)

				for bp.Next() {
					tu.ProcessRequest(t, router, ds.PK_HTTP_VERB, url, reqBody, http.StatusOK, "")
				}
			})
			t.StopTimer()

			speed := float64(t.N) / time.Since(start).Seconds()
			ns := float64(time.Since(start).Nanoseconds()) / float64(t.N)

			fmt.Printf("Throughput %f ops/sec\n.", speed)
			fmt.Printf("Latency  %f ns/op\n", ns)
		})
}

func createReq(maxRows, opCount int, operationId string) string {
	rowId := opCount % maxRows
	//fmt.Printf("Operation ID: %s, Reading row %d\n", operationId, rowId)
	col := "id0"
	param := ds.PKReadBody{
		Filters:     tu.NewFilter(&col, rowId),
		ReadColumns: tu.NewReadColumns("col_", 1),
		OperationID: &operationId,
	}
	body, _ := json.Marshal(param)
	return string(body)
}

func BenchmarkMT(b *testing.B) {

	start := time.Now()
	fmt.Printf("-------- b.N: %d ---------\n", b.N)
	db := "bench"
	table := "table_1"
	maxRows := 1000
	tu.WithDBs(b, [][][]string{common.Database(db)},
		[]tu.RegisterTestHandler{RegisterPKTestHandler, stat.RegisterStatTestHandler}, func(router *gin.Engine) {

			b.ResetTimer()
			threads := 1
			link := make(chan int, threads)
			donearr := make([]chan bool, threads)
			for i := 0; i < threads; i++ {
				donearr[i] = make(chan bool)
			}
			numOps := b.N

			go producer1(b, router, numOps, link)

			for i := 0; i < threads; i++ {
				go consumer1(b, router, i, db, table, maxRows, link, donearr[i])
			}

			for i := 0; i < threads; i++ {
				<-donearr[i]
			}

			b.StopTimer()

			speed := float64(numOps) / time.Since(start).Seconds()
			ns := float64(time.Since(start).Nanoseconds()) / float64(b.N)
			fmt.Printf("Speed %f\n", speed)
			fmt.Printf("Speed %f ns/op\n", ns)
		})
}

func producer1(b testing.TB, router *gin.Engine, numOps int, link chan int) {
	for i := 0; i < numOps; i++ {
		link <- i
	}
	close(link)
}

func consumer1(b testing.TB, router *gin.Engine, id int, db string, table string, maxRowID int, link chan int, done chan bool) {
	for opId := range link {
		rowId := opId % maxRowID
		url := tu.NewPKReadURL(db, table)
		col := "id0"
		param := ds.PKReadBody{
			Filters:     tu.NewFilter(&col, rowId),
			ReadColumns: tu.NewReadColumns("col_", 1),
			OperationID: tu.NewOperationID(5),
		}
		body, _ := json.Marshal(param)

		tu.ProcessRequest(b, router, ds.PK_HTTP_VERB, url, string(body), http.StatusOK, "")
		// stats, _ := stat.Stats()
		// fmt.Printf("Thread %d, Stats: %v\n", id, *stats)
	}
	done <- true
}
