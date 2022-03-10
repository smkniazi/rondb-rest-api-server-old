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
	"zappem.net/pub/debug/xxd"
)

const BUFFER_SIZE = 512

func makeNativeRequest() error {

	// req := C.malloc(C.size_t(size))
	// cBuf := (*[size]byte)(req)

	return nil

}

//  PK READ Request Buffer
//  ======================
//
//  HEADER
//  ======
//  [   4B   ][   4B   ][   4B   ][   4B   ][   4B   ][   4B   ][   4B   ][   4B   ][   4B   ] ....
//    Type     Capacity  Length     DB         Table      PK     Read Cols    Op_ID    TX_ID
//                               Offset      Offset    Offset     Offset     Offset   Offset
//  BODY
//  ====
//  [ bytes ... ] ...
//    Null termnated DB Name
//
//  [ bytes ... ] ...
//    Null termnated Table Name
//
//  [   4B   ][   4B   ][   4B   ][   4B   ][   4B   ][   bytes ...  ][   4B   ][   4B   ] ....
//    Count     kv 1       kv2      key       value     key value
//            offset     offset    offset     offset
//
//  [   4B   ][   bytes ... ] ....
//    Count     key value pairs
//
//  [ bytes ... ] ...
//    null terminated  operation Id
//
//  [ bytes ... ] ...
//   transaction Id

func PKReadDB(pkrParams *PKReadParams) {

	buffer := C.malloc(C.size_t(BUFFER_SIZE))
	iBuf := (*[BUFFER_SIZE / 4]uint32)(buffer)
	bBuf := (*[BUFFER_SIZE]byte)(buffer)

	// First N bytes are for header
	var head uint32 = C.PKR_HEADER_END

	dbOffSet := head
	head = copyGoString([]byte(*pkrParams.DB), bBuf, head)

	tableOffSet := head
	head = copyGoString([]byte(*pkrParams.Table), bBuf, head)

	// PK Filters
	head = align(head)
	pkOffset := head
	iBuf[head/4] = uint32(len(*pkrParams.Filters))
	head += 4

	kvi := head / 4 // index for storing offsets for each key/value pair
	// skip for N number of offsets one for each key/value pair
	head = head + (uint32(len(*pkrParams.Filters)) * 4)
	for _, filter := range *pkrParams.Filters {
		head = align(head)

		tupleOffset := head

		head = head + 8 //  for key and value offsets
		keyOffset := head
		head = copyGoString([]byte(*filter.Column), bBuf, head)
		valueOffset := head
		head = copyGoString([]byte(*filter.Value), bBuf, head)

		iBuf[kvi] = tupleOffset
		kvi++
		iBuf[tupleOffset/4] = keyOffset
		iBuf[(tupleOffset/4)+1] = valueOffset
	}

	// Read Columns
	head = align(head)
	readColsOffset := head
	iBuf[head/4] = uint32(len(*pkrParams.ReadColumns))
	head += 4

	rci := head / 4 // index for storing ofsets for each read column
	// skip for N number of offsets one for each column name
	head = head + (uint32(len(*pkrParams.ReadColumns)) * 4)

	for _, col := range *pkrParams.ReadColumns {
		iBuf[rci] = head
		rci++
		// fmt.Printf("Read col offset %d\n", head)
		head = copyGoString([]byte(col), bBuf, head)
	}

	// Operation ID
	opIdOffset := head
	head = copyGoString([]byte(*pkrParams.OperationID), bBuf, head)

	// Header
	iBuf[C.PKR_OP_TYPE_IDX] = uint32(C.RDRS_PK_REQ_ID)
	iBuf[C.PKR_CAPACITY_IDX] = uint32(BUFFER_SIZE)
	iBuf[C.PKR_LENGTH_IDX] = uint32(head)
	iBuf[C.PKR_DB_IDX] = uint32(dbOffSet)
	iBuf[C.PKR_TABLE_IDX] = uint32(tableOffSet)
	iBuf[C.PKR_PK_COLS_IDX] = uint32(pkOffset)
	iBuf[C.PKR_READ_COLS_IDX] = uint32(readColsOffset)
	iBuf[C.PKR_OP_ID_IDX] = uint32(opIdOffset)

	xxd.Print(0, bBuf[:])

	C.helloWorld((*C.char)(buffer))
}

func copyGoString(src []byte, dst *[BUFFER_SIZE]byte, offset uint32) uint32 {
	for i, j := offset, 0; i < (offset + uint32(len(src))); i, j = i+1, j+1 {
		(*dst)[i] = src[j]
	}
	(*dst)[offset+uint32(len(src))] = 0x00
	return offset + uint32(len(src)) + 1
	// return 0
}

func align(head uint32) uint32 {
	a := head % 4
	if a != 0 {
		head += (4 - a)
	}
	return head
}
