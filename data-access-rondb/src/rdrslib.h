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

typedef struct RDRSRet {
  int ret_code;
  char *message;
} RDRSRet;

/**
 * Initialize connection to the database
 */
RDRSRet init(const char *connection_string);

/**
 * Primary key read operation
 *
 * @db database
 * @table table
 * @pkCols char array of primary key column names
 * @values char array of primary key column values
 * @readCols re
 */
RDRSRet pkRead(const char *db, const char *table, const char **pkCols,
               const char **values, const char **readCols);
/**
 * hello work function for testing
 */
RDRSRet helloWorld();
#ifdef __cplusplus
}
#endif

