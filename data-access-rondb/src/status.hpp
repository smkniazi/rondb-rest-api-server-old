#include "rdrslib.h"
#include <NdbApi.hpp>
#include <cstring>
#include <string>

using namespace std;
/**
 * create an object of RS_Status.
 * Note it is the receiver responsibility to free the memory
 */
inline RS_Status RS_ERROR(const int code, string msg) {
  char *charArr = nullptr;
  if (!msg.empty()) {
    charArr = new char[msg.length() + 1];
    strcpy(charArr, msg.c_str());
  }
  RS_Status ret = {code, charArr};
  return ret;
}

inline RS_Status RS_ERROR(const struct NdbError &error, string msg) {
  string userMsg = "Error: " + msg + " Error: code:" + to_string(error.code) +
                   " MySQL Code: " + to_string(error.mysql_code) + " Message: " + error.message +
                   ((error.classification == NdbError::NoDataFound) ? "No data" : "");
  return RS_ERROR(error.code != 0 ? error.code : 1, userMsg);
}

#define RS_OK RS_ERROR(0, "")
