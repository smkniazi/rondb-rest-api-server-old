/*
 * Copyright (C) 2022 Hopsworks AB
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
 * USA.
 */

#ifndef DATA_ACCESS_RONDB_SRC_RDRS_CONST_H_
#define DATA_ACCESS_RONDB_SRC_RDRS_CONST_H_

#ifdef __cplusplus
extern "C" {
#endif

// 4 bytes. Max addressable memrory is 4GB
// which is max supported blob size
#define ADDRESS_SIZE 4

// Request Type Identifiers
#define RDRS_PK_REQ_ID    1
#define RDRS_BATCH_REQ_ID 2

// Primary Key Read Request Header Indexes
#define PKR_OP_TYPE_IDX   0
#define PKR_CAPACITY_IDX  1
#define PKR_LENGTH_IDX    2
#define PKR_DB_IDX        3
#define PKR_TABLE_IDX     4
#define PKR_PK_COLS_IDX   5
#define PKR_READ_COLS_IDX 6
#define PKR_OP_ID_IDX     7
#define PKR_HEADER_END    32

#ifdef __cplusplus
}
#endif
#endif  // DATA_ACCESS_RONDB_SRC_RDRS_CONST_H_
