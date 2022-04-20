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

/*
#include "./../../../../../data-access-rondb/src/rdrs-const.h"
#include "./../../../../../data-access-rondb/src/rdrs-dal.h"
*/
import "C"
import (
	"fmt"
	"math"
	"unsafe"

	"hopsworks.ai/rdrs/internal/common"
	"hopsworks.ai/rdrs/internal/dal"
	ds "hopsworks.ai/rdrs/internal/datastructs"
)

// Also checkout internal/router/handler/pkread/encoding-scheme.png

//  PK READ Request
//  ===============
//
//  HEADER
//  ======
//  [   4B   ][   4B   ][   4B   ][   4B   ][   4B   ][   4B   ][   4B   ][   4B   ][   4B   ] ....
//    Type     Capacity  Length     DB         Table      PK     Read Cols    Op_ID    TX_ID
//                               Offset      Offset    Offset     Offset     Offset   Offset
//  BODY
//  ====
//  [ bytes ... ]
//    Null termnated DB Name
//
//  [ bytes ... ]
//    Null termnated Table Name
//
//  [   4B   ][   4B   ]...[   4B   ][   4B   ][   4B   ][   bytes ...  ][ 2B ] [ bytes... ][   4B   ][   4B   ] ....
//    Count     kv 1          kv n       key       value     key          val     val
//            offset        offset     offset     offset                 size
//                                      ^
//              ________________________|                                                     ^
//                           _________________________________________________________________|
//
//
//  [   4B   ] [  4B     ] [  4B     ] ...
//    Count   col1 offset   col2 offset
//
//  [  4B ] [   bytes ... ] [  4B ] [   bytes ... ] ...
//  type     null terminated column names
//
//  [ bytes ... ] ...
//    null terminated  operation Id
//

func createNativeRequest(pkrParams *ds.PKReadParams) (*dal.Native_Buffer, *dal.Native_Buffer, error) {
	response := dal.GetBuffer()
	request := dal.GetBuffer()
	iBuf := unsafe.Slice((*uint32)(request.Buffer), request.Size)

	// First N bytes are for header
	var head uint32 = C.PKR_HEADER_END

	dbOffSet := head

	head, err := common.CopyGoStrToCStr([]byte(*pkrParams.DB), request, head)
	if err != nil {
		return nil, nil, err
	}

	tableOffSet := head
	head, err = common.CopyGoStrToCStr([]byte(*pkrParams.Table), request, head)
	if err != nil {
		return nil, nil, err
	}

	// PK Filters
	head = common.AlignWord(head)
	pkOffset := head
	iBuf[head/C.ADDRESS_SIZE] = uint32(len(*pkrParams.Filters))
	head += C.ADDRESS_SIZE

	kvi := head / C.ADDRESS_SIZE // index for storing offsets for each key/value pair
	// skip for N number of offsets one for each key/value pair
	head = head + (uint32(len(*pkrParams.Filters)) * C.ADDRESS_SIZE)
	for _, filter := range *pkrParams.Filters {
		head = common.AlignWord(head)

		tupleOffset := head

		head = head + 8 //  for key and value offsets
		keyOffset := head
		head, err = common.CopyGoStrToCStr([]byte(*filter.Column), request, head)
		if err != nil {
			return nil, nil, err
		}
		valueOffset := head
		head, err = common.CopyGoStrToNDBStr(*filter.Value, request, head)
		if err != nil {
			return nil, nil, err
		}

		iBuf[kvi] = tupleOffset
		kvi++
		iBuf[tupleOffset/C.ADDRESS_SIZE] = keyOffset
		iBuf[(tupleOffset/C.ADDRESS_SIZE)+1] = valueOffset
	}

	// Read Columns
	head = common.AlignWord(head)
	var readColsOffset uint32 = 0
	if pkrParams.ReadColumns != nil {
		readColsOffset = head
		iBuf[head/C.ADDRESS_SIZE] = uint32(len(*pkrParams.ReadColumns))
		head += C.ADDRESS_SIZE

		rci := head / C.ADDRESS_SIZE // index for storing ofsets for each read column
		// skip for N number of offsets one for each column name
		head = head + (uint32(len(*pkrParams.ReadColumns)) * C.ADDRESS_SIZE)

		for _, col := range *pkrParams.ReadColumns {
			head = common.AlignWord(head)

			iBuf[rci] = head
			rci++

			// return type
			var drt uint32 = C.DEFAULT_DRT
			if col.DataReturnType != nil {
				drt, err = dataReturnType(col.DataReturnType)
				if err != nil {
					return nil, nil, err
				}
			}

			iBuf[head/C.ADDRESS_SIZE] = drt
			head += C.ADDRESS_SIZE

			// col name
			head, err = common.CopyGoStrToCStr([]byte(*col.Column), request, head)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	// Operation ID
	var opIdOffset uint32 = 0
	if pkrParams.OperationID != nil {
		opIdOffset = head
		head, err = common.CopyGoStrToCStr([]byte(*pkrParams.OperationID), request, head)
		if err != nil {
			return nil, nil, err
		}
	}

	// request buffer header
	iBuf[C.PKR_OP_TYPE_IDX] = uint32(C.RDRS_PK_REQ_ID)
	iBuf[C.PKR_CAPACITY_IDX] = uint32(request.Size)
	iBuf[C.PKR_LENGTH_IDX] = uint32(head)
	iBuf[C.PKR_DB_IDX] = uint32(dbOffSet)
	iBuf[C.PKR_TABLE_IDX] = uint32(tableOffSet)
	iBuf[C.PKR_PK_COLS_IDX] = uint32(pkOffset)
	iBuf[C.PKR_READ_COLS_IDX] = uint32(readColsOffset)
	iBuf[C.PKR_OP_ID_IDX] = uint32(opIdOffset)

	//response buffer header
	respBuf := unsafe.Slice((*uint32)(response.Buffer), response.Size)
	respBuf[C.PKR_OP_TYPE_IDX] = uint32(C.RDRS_PK_REQ_ID)
	respBuf[C.PKR_CAPACITY_IDX] = uint32(response.Size)
	respBuf[C.PKR_LENGTH_IDX] = uint32(C.ADDRESS_SIZE * 2)
	//xxd.Print(0, bBuf[:])
	return request, response, nil
}

func processResponse(buffer unsafe.Pointer) string {
	return C.GoString((*C.char)(buffer))
}

func dataReturnType(drt *string) (uint32, error) {
	if *drt == ds.DRT_DEFAULT {
		return C.DEFAULT_DRT, nil
	} else {
		return math.MaxUint32, fmt.Errorf("Return data type is not supported. Data type: " + *drt)
	}
}
