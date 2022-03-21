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
 * only for testing
 */
int main(int argc, char **argv) {
  char connection_string[] = "localhost:1186";
  init(connection_string);
  /* pkRead(nullptr); */
  this_thread::sleep_for(chrono::milliseconds(1000));
  return 0;
}

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
    return RS_ERROR(SERVER_ERROR, ERR012 + string(" RetCode: ") + to_string(retCode));
  }

  ndb_connection = new Ndb_cluster_connection(connection_string);
  retCode        = ndb_connection->connect();
  if (retCode != 0) {
    return RS_ERROR(SERVER_ERROR, ERR013 + string(" RetCode: ") + to_string(retCode));
  }

  retCode = ndb_connection->wait_until_ready(30, 0);
  if (retCode != 0) {
    return RS_ERROR(SERVER_ERROR, ERR014 + string(" RetCode: ") + to_string(retCode));
  }

  INFO("Connected.")
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
    return RS_ERROR(SERVER_ERROR, ERR015 + string(" RetCode: ") + to_string(retCode));

  }
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
  if (status.http_code != SUCCESS) {
    return status;
  }

  return RS_OK;
}

RS_Status pkRead2(const char *request) {
  //  /* int tid =  pthread_self(); */
  //  /* INFO("Hello World! " << tid ) */
  //  PKRead pkread(request);
  //  INFO("Type:  \"" << pkread.operationType() << "\"")
  //  INFO("DB:  \"" << pkread.db() << "\"")
  //  INFO("Table:  \"" << pkread.table() << "\"")
  //  uint32_t count = pkread.pkColumnsCount();
  //  for (uint32_t i = 0; i < count; i++) {
  //    INFO("PK Name:  \"" << pkread.pkName(i) << "\"")
  //    INFO("PK Value:  \"" << pkread.pkValue(i) << "\"")
  //  }
  //
  //  uint32_t rcols = pkread.readColumnsCount();
  //  for (uint32_t i = 0; i < rcols; i++) {
  //    INFO("Read Column:  \"" << pkread.readColumnName(i) << "\"")
  //  }
  //  INFO("Operation ID:  \"" << pkread.operationId() << "\"")

  return RS_OK;
}
