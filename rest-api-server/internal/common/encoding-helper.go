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

package common

import (
	"fmt"
)

// copy a go string to the buffer at the specified location.
// NULL is appended to the string for c/c++ compatibility
func CopyGoStrToCStr(src []byte, dst []byte, offset uint32, capacity uint32) (uint32, error) {
	if offset+uint32(len(src))+1 > capacity {
		return 0, fmt.Errorf("Trying to write more data than the buffer capacity")
	}

	for i, j := offset, 0; i < (offset + uint32(len(src))); i, j = i+1, j+1 {
		dst[i] = src[j]
	}
	dst[offset+uint32(len(src))] = 0x00
	return offset + uint32(len(src)) + 1, nil
}

// copy a go string to the buffer at the specified location.
// first two bytes store the size of the string
// and NULL is appended to the string for c/c++ compatibility.
//
// Note: at this moment we do not know the column type.
// NdbDictionary::Column::ArrayTypeFixed uses 0 bytes for length
// NdbDictionary::Column::ArrayTypeShortVar uses 1 byte for length
// NdbDictionary::Column::ArrayTypeMediumVar uses 2 bytes for length
// for now we store the length in 2 bytes. Later in the native layer
// we adjust the size accordingly.

func CopyGoStrToNDBStr(src []byte, dst []byte, offset uint32, capacity uint32) (uint32, error) {
	if offset+uint32(len(src))+1+2 > capacity {
		return 0, fmt.Errorf("Trying to write more data than the buffer capacity")
	}

	dst[offset] = byte(len(src) % 256)
	dst[offset+1] = byte(len(src) / 256)
	offset += 2

	for i, j := offset, 0; i < (offset + uint32(len(src))); i, j = i+1, j+1 {
		dst[i] = src[j]
	}
	dst[offset+uint32(len(src))] = 0x00
	return offset + uint32(len(src)) + 1, nil
}

// WORD alignment
func AlignWord(head uint32) uint32 {
	a := head % 4
	if a != 0 {
		head += (4 - a)
	}
	return head
}
