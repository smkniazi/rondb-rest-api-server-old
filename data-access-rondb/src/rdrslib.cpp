#include <./src/rdrslib.h>
#include <./src/logger.h>
#include <NdbApi.hpp>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <iterator>
#include <pthread.h>


inline RDRSRet mkRDRSRet(const int code, const char *msg) {
  /* char cmsg[] = *msg; */
  /* const char cmsg[] = "test"; */ 
  RDRSRet ret = {code, (char*)msg};
  return ret;
}

Ndb_cluster_connection *ndb_connection;
Ndb *ndb_object;

int main(int argc, char **argv) {
  char connection_string[] = "localhost:1186";
  /* init(connection_string); */
  helloWorld();
  return 0;
}

RDRSRet init(const char *connection_string) {
  int retCode = 0;
  TRACE("Connecting to " << connection_string << " ... ")

  retCode = ndb_init();
  if (retCode != 0) {
    return mkRDRSRet(retCode, "ndb_init() failed");
  }

  ndb_connection = new Ndb_cluster_connection(connection_string);
  retCode = ndb_connection->connect();
  if (retCode != 0) {
    return mkRDRSRet(retCode, "failed to connect to RonDB mgm server");
  }

  retCode = ndb_connection->wait_until_ready(30, 0);
  if (retCode != 0) {
    return mkRDRSRet(retCode, "Cluster was not ready within 30 secs");
  }

  ndb_object = new Ndb(ndb_connection, "test_db");
  retCode = ndb_object->init();
  if (retCode != 0) {
    return mkRDRSRet(retCode, "Failed to initialize ndb object");
  }

  INFO("Connected.")
  return mkRDRSRet(0, NULL);
}

RDRSRet pkRead(const char *db, const char *table, const char **pkCols,
               const char **values, const char **readCols) {
  return mkRDRSRet(0, NULL);
}

RDRSRet helloWorld() {
  int tid =  pthread_self();
  INFO("Hello World! " << tid )
  return mkRDRSRet(1111, "some message");
}
