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

#include "src/rdrs-dal.h"
#include <cstdlib>
#include <cstring>
#include <string>
#include <iostream>
#include <iterator>
#include <sstream>
#include <NdbApi.hpp>
#include "src/error-strs.h"
#include "src/logger.hpp"
#include "src/pk-read/pkr-operation.hpp"
#include "src/status.hpp"

Ndb_cluster_connection *ndb_connection;

/**
 * Initialize NDB connection
 * @param connection_string NDB connection string {url}:{port}
 * @return status
 */
RS_Status Init(const char *connection_string) {
  int retCode = 0;
  TRACE("Connecting to " << connection_string << " ... ")

  retCode = ndb_init();
  if (retCode != 0) {
    return RS_SERVER_ERROR(ERROR_001 + std::string(" RetCode: ") + std::to_string(retCode));
  }

  ndb_connection = new Ndb_cluster_connection(connection_string);
  retCode        = ndb_connection->connect();
  if (retCode != 0) {
    return RS_SERVER_ERROR(ERROR_002 + std::string(" RetCode: ") + std::to_string(retCode));
  }

  retCode = ndb_connection->wait_until_ready(30, 0);
  if (retCode != 0) {
    return RS_SERVER_ERROR(ERROR_003 + std::string(" RetCode: ") + std::to_string(retCode));
  }

  INFO("Connected.")
  return RS_OK;
}

RS_Status Shutdown() {
  try {
    // ndb_end(0); // causes seg faults when called repeated from unit tests*/
    delete ndb_connection;
  } catch (...) {
    std::cout << "------> Exception in Shutdown <------" << std::endl;
  }
  return RS_OK;
}

/**
 * Creats a new NDB Object
 *
 * @param[in] ndb_connection
 * @param[out] ndb_object
 *
 * @return status
 */
RS_Status GetNDBObject(Ndb_cluster_connection *ndb_connection, Ndb **ndb_object) {
  *ndb_object  = new Ndb(ndb_connection);
  int retCode = (*ndb_object)->init();
  if (retCode != 0) {
    return RS_SERVER_ERROR(ERROR_004 + std::string(" RetCode: ") + std::to_string(retCode));
  }
  return RS_OK;
}

/**
 * Closes a NDB Object
 *
 * @param[int] ndb_object
 *
 * @return status
 */
RS_Status CloseNDBObject(Ndb **ndb_object) {
  delete *ndb_object;
  return RS_OK;
}

RS_Status PKRead(char *reqBuff, char *respBuff) {
  Ndb *ndb_object   = nullptr;
  RS_Status status = GetNDBObject(ndb_connection, &ndb_object);
  if (status.http_code != SUCCESS) {
    return status;
  }

  PKROperation pkread(reqBuff, respBuff, ndb_object);

  status = pkread.PerformOperation();
  CloseNDBObject(&ndb_object);
  if (status.http_code != SUCCESS) {
    return status;
  }

  return RS_OK;
}

/**
 * only for testing
 */
int main(int argc, char **argv) {
  char connection_string[] = "localhost:1186";
  return 0;
}

