#include "error-strs.h"
#include "logger.hpp"
#include "pk-read/pkr-operation.hpp"
#include "status.hpp"
#include <NdbApi.hpp>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <iterator>
#include <pthread.h>
#include <sstream>
#include <string.h>
#include <thread>

using namespace std;

Ndb_cluster_connection *ndb_connection;

/**
 * Initialize NDB connection
 * @param connection_string NDB connection string {url}:{port}
 * @return status
 */
RS_Status init(const char *connection_string) {
  int retCode = 0;
  TRACE("Connecting to " << connection_string << " ... ")

  retCode = ndb_init();
  if (retCode != 0) {
    return RS_ERROR(SERVER_ERROR, ERROR_001 + string(" RetCode: ") + to_string(retCode));
  }

  ndb_connection = new Ndb_cluster_connection(connection_string);
  retCode        = ndb_connection->connect();
  if (retCode != 0) {
    return RS_ERROR(SERVER_ERROR, ERROR_002 + string(" RetCode: ") + to_string(retCode));
  }

  retCode = ndb_connection->wait_until_ready(30, 0);
  if (retCode != 0) {
    return RS_ERROR(SERVER_ERROR, ERROR_003 + string(" RetCode: ") + to_string(retCode));
  }

  INFO("Connected.")
  return RS_OK;
}

RS_Status shutdown() {
  try {
    ndb_end(0);
    delete ndb_connection;
  } catch (...) {
    cout << "------> Exception in Shutdown <------" << endl;
  }
  return RS_OK;
}

/**
 * Creats a new NDB Object
 *
 * @param[in] ndb_connection
 * @param[out] ndbObject
 *
 * @return status
 */
RS_Status getNDBObject(Ndb_cluster_connection *ndb_connection, Ndb **ndbObject) {
  *ndbObject  = new Ndb(ndb_connection);
  int retCode = (*ndbObject)->init();
  if (retCode != 0) {
    return RS_ERROR(SERVER_ERROR, ERROR_004 + string(" RetCode: ") + to_string(retCode));
  }
  return RS_OK;
}

/**
 * Closes a NDB Object
 *
 * @param[int] ndbObject
 *
 * @return status
 */
RS_Status closeNDBObject(Ndb **ndbObject) {
  delete *ndbObject;
  return RS_OK;
}

RS_Status pkRead(char *reqBuff, char *respBuff) {

  Ndb *ndbObject   = nullptr;
  RS_Status status = getNDBObject(ndb_connection, &ndbObject);
  if (status.http_code != SUCCESS) {
    return status;
  }

  PKROperation pkread(reqBuff, respBuff, ndbObject);

  status = pkread.performOperation();
  closeNDBObject(&ndbObject);
  if (status.http_code != SUCCESS) {
    return status;
  }

  return RS_OK;
}

/**
 * only for testing
 */
int main(int argc, char **argv) {
  for (int i = 0; i < 10; i++) {
    char connection_string[] = "localhost:1186";
    init(connection_string);

    Ndb *ndbObject   = nullptr;
    RS_Status status = getNDBObject(ndb_connection, &ndbObject);
    closeNDBObject(&ndbObject);
    shutdown();
    /* pkRead(nullptr); */
    this_thread::sleep_for(chrono::milliseconds(1000));
  }
  return 0;
}

