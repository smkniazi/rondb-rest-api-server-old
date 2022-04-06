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
#include <mgmapi.h>
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

int GetAvailableAPINode(const char *connection_string);

Ndb_cluster_connection *ndb_connection;

/**
 * Initialize NDB connection
 * @param connection_string NDB connection string {url}:{port}
 * @param find_available_node_ID if set to 1 then we will first find an available node id to
 * connect to
 * @return status
 */
RS_Status Init(const char *connection_string, _Bool find_available_node_id) {
  int retCode = 0;
  TRACE("Connecting to " << connection_string << " ... ")

  retCode = ndb_init();
  if (retCode != 0) {
    return RS_SERVER_ERROR(ERROR_001 + std::string(" RetCode: ") + std::to_string(retCode));
  }

  int node_id = -1;
  if (find_available_node_id == true) {
    node_id = GetAvailableAPINode(connection_string);
    if (node_id == -1) {
      return RS_SERVER_ERROR(ERROR_024);
    }
  }

  if (node_id != -1) {
    ndb_connection = new Ndb_cluster_connection(connection_string, node_id);
  } else {
    ndb_connection = new Ndb_cluster_connection(connection_string);
  }
  retCode = ndb_connection->connect();
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
  *ndb_object = new Ndb(ndb_connection);
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
  Ndb *ndb_object  = nullptr;
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


static int LastConnectedInodeID = -1;
/*
 * NDB API does not support gracefull disconnection form the  
 * cluster. All disconnections are treated as failures. When
 * you disconnect, the API node is not able to accept new 
 * connections until the filure recovery has completed for
 * the API node. This can take upto ~5 sec, slowing down 
 * unit tests which start/stop the NDB API multiple times.
 * This function returns next available API node to connect to.
 */
int GetAvailableAPINode(const char *connection_string) {
  NdbMgmHandle h;

  h = ndb_mgm_create_handle();
  if (h == 0) {
    WARN("Failed to create mgm handle");
    return -1;
  }

  if (ndb_mgm_set_connectstring(h, connection_string) == -1) {
    WARN("Failed set mgm connect string");
    return -1;
  }

  if (ndb_mgm_connect(h, 0, 0, 0)) {
    INFO("Failed to connect to mgm node");
    return -1;
  }

  // look for api nodes only
  ndb_mgm_node_type node_types[2]   = {NDB_MGM_NODE_TYPE_API, NDB_MGM_NODE_TYPE_UNKNOWN};
  struct ndb_mgm_cluster_state *ret = ndb_mgm_get_status2(h, node_types);

  if (ret->no_of_nodes > 1) {
    int max_node_id = ret->node_states[0].node_id;
    for (int i = 1; i < ret->no_of_nodes; i++) {
      if (ret->node_states[i].node_id > max_node_id) {
        max_node_id = ret->node_states[i].node_id;
      }
    }

    if (LastConnectedInodeID == max_node_id) {
      LastConnectedInodeID = -1;
    }

    for (int i = 0; i < ret->no_of_nodes; i++) {
      if (ret->node_states[i].node_id > LastConnectedInodeID &&
          ret->node_states[i].node_status == NDB_MGM_NODE_STATUS_NO_CONTACT) {
        LastConnectedInodeID = ret->node_states[i].node_id;
        free(ret);
        return LastConnectedInodeID;
      }
    }
  }

  free(ret);
  return -1;
}

/**
 * only for testing
 */
int main(int argc, char **argv) {
  ndb_init();
  char connection_string[] = "localhost:1186";
  INFO(std::string("Free node is ") + std::to_string(GetAvailableAPINode(connection_string)));
  ndb_end(0);
}

