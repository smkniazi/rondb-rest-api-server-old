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

package native

// copy a go string to a buffer at the specified location. Null is appended to the string
// for c/c++ compatibility
func CopyGoString(src []byte, dst *[BUFFER_SIZE]byte, offset uint32) uint32 {
	for i, j := offset, 0; i < (offset + uint32(len(src))); i, j = i+1, j+1 {
		(*dst)[i] = src[j]
	}
	(*dst)[offset+uint32(len(src))] = 0x00
	return offset + uint32(len(src)) + 1
	// return 0
}

// WORD alignment
func AlignWord(head uint32) uint32 {
	a := head % 4
	if a != 0 {
		head += (4 - a)
	}
	return head
}
