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
#cgo CFLAGS: -g -Wall
#cgo LDFLAGS: -L./../../../../../data-access-rondb/build/ -lrdrclient
#cgo LDFLAGS: -L/usr/local/mysql/lib -lndbclient
#include <stdlib.h>
#include "./../../../../../data-access-rondb/src/rdrslib.h"
#include "./../../../../../data-access-rondb/src/rdrs-const.h"
*/
import "C"
import (
	"unsafe"

	"hopsworks.ai/rdrs/internal/native"
)

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
//  [   4B   ][   4B   ]...[   4B   ][   4B   ][   4B   ][   bytes ...  ][   4B   ][   4B   ] ....
//    Count     kv 1          kv n       key       value     key value      key       value     key value
//            offset        offset     offset     offset       pair       offset     offset       pair
//                                      ^
//              ________________________|
//                                                                          ^
//                           _______________________________________________|
//
//  [   4B   ][   bytes ... ] ....
//    Count     null terminated column names
//
//  [ bytes ... ] ...
//    null terminated  operation Id
//
//  [ bytes ... ] ...
//   null terminated transaction Id

func createNativeRequest(pkrParams *PKReadParams) unsafe.Pointer {
	buffer := native.GetBuffer()

	iBuf := (*[native.BUFFER_SIZE / C.ADDRESS_SIZE]uint32)(buffer)
	bBuf := (*[native.BUFFER_SIZE]byte)(buffer)

	// First N bytes are for header
	var head uint32 = C.PKR_HEADER_END

	dbOffSet := head
	head = native.CopyGoString([]byte(*pkrParams.DB), bBuf, head)

	tableOffSet := head
	head = native.CopyGoString([]byte(*pkrParams.Table), bBuf, head)

	// PK Filters
	head = native.AlignWord(head)
	pkOffset := head
	iBuf[head/C.ADDRESS_SIZE] = uint32(len(*pkrParams.Filters))
	head += C.ADDRESS_SIZE

	kvi := head / C.ADDRESS_SIZE // index for storing offsets for each key/value pair
	// skip for N number of offsets one for each key/value pair
	head = head + (uint32(len(*pkrParams.Filters)) * C.ADDRESS_SIZE)
	for _, filter := range *pkrParams.Filters {
		head = native.AlignWord(head)

		tupleOffset := head

		head = head + 8 //  for key and value offsets
		keyOffset := head
		head = native.CopyGoString([]byte(*filter.Column), bBuf, head)
		valueOffset := head
		head = native.CopyGoString([]byte(*filter.Value), bBuf, head)

		iBuf[kvi] = tupleOffset
		kvi++
		iBuf[tupleOffset/C.ADDRESS_SIZE] = keyOffset
		iBuf[(tupleOffset/C.ADDRESS_SIZE)+1] = valueOffset
	}

	// Read Columns
	head = native.AlignWord(head)
	var readColsOffset uint32 = 0
	if pkrParams.ReadColumns != nil {
		readColsOffset = head
		iBuf[head/C.ADDRESS_SIZE] = uint32(len(*pkrParams.ReadColumns))
		head += C.ADDRESS_SIZE

		rci := head / C.ADDRESS_SIZE // index for storing ofsets for each read column
		// skip for N number of offsets one for each column name
		head = head + (uint32(len(*pkrParams.ReadColumns)) * C.ADDRESS_SIZE)

		for _, col := range *pkrParams.ReadColumns {
			iBuf[rci] = head
			rci++
			// fmt.Printf("Read col offset %d\n", head)
			head = native.CopyGoString([]byte(col), bBuf, head)
		}
	}

	// Operation ID
	var opIdOffset uint32 = 0
	if pkrParams.OperationID != nil {
		opIdOffset = head
		head = native.CopyGoString([]byte(*pkrParams.OperationID), bBuf, head)
	}

	// Header
	iBuf[C.PKR_OP_TYPE_IDX] = uint32(C.RDRS_PK_REQ_ID)
	iBuf[C.PKR_CAPACITY_IDX] = uint32(native.BUFFER_SIZE)
	iBuf[C.PKR_LENGTH_IDX] = uint32(head)
	iBuf[C.PKR_DB_IDX] = uint32(dbOffSet)
	iBuf[C.PKR_TABLE_IDX] = uint32(tableOffSet)
	iBuf[C.PKR_PK_COLS_IDX] = uint32(pkOffset)
	iBuf[C.PKR_READ_COLS_IDX] = uint32(readColsOffset)
	iBuf[C.PKR_OP_ID_IDX] = uint32(opIdOffset)
	return buffer
}
