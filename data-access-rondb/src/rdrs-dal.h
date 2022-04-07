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

#ifdef __cplusplus
extern "C" {
#endif

#ifndef DATA_ACCESS_RONDB_SRC_RDRS_DAL_H_
#define DATA_ACCESS_RONDB_SRC_RDRS_DAL_H_

#include <stdbool.h>

typedef enum HTTP_CODE {
  SUCCESS      = 200,
  CLIENT_ERROR = 400,
  NOT_FOUND    = 404,
  SERVER_ERROR = 500
} HTTP_CODE;

typedef struct RS_Status {
  HTTP_CODE http_code;  // rest server return code. 200 for successful operation
  int status;           // NdbError.ndberror_status_enum
  int classification;   // NdbError.ndberror_classification_enum
  int code;             // NdbError.code
  int mysql_code;       // NdbError.mysql_code
  char *message;        // REST server message. NOTE: receiver's responsibility to free this memory
  int err_line_no;
  char *err_file_name;  // NOTE: receiver's responsibility to free this memory
} RS_Status;

// Data return type you can change the return type for the column data
// int/floats/decimal are returned as JSON Number type (default),
// varchar/char are returned as strings (default) and varbinary as base64 (default)
// Right now only default return type is supported
typedef enum DataReturnType {
  DEFAULT_DRT = 1,
  // BASE64 = 2;

  __MAX_TYPE_NOT_A_DRT = 1
} DataReturnType;

/**
 * Initialize connection to the database
 */
RS_Status Init(const char *connection_string, _Bool find_available_node_id);

/**
 * Shutdown connection
 */
RS_Status Shutdown();

/**
 * Primary key read operation
 */
RS_Status PKRead(char *reqBuff, char *respBuff);

#endif

#ifdef __cplusplus
}
#endif  // DATA_ACCESS_RONDB_SRC_RDRS_DAL_H_

