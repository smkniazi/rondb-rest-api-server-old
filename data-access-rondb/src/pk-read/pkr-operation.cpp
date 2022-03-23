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

#include "pkr-operation.hpp"
#include "pkr-request.hpp"
#include "pkr-response.hpp"
#include "src/error-strs.h"
#include "src/logger.hpp"
#include "src/rdrs-const.h"
#include "src/status.hpp"

using namespace std;

PKROperation::PKROperation(char *reqBuff, char *respBuff, Ndb *ndbObject)
    : request(reqBuff), response(respBuff) {
  this->ndbObject = ndbObject;
}

/**
 * start a transaction
 *
 * @param[in] ndbObject
 * @param[in] pkread
 * @param[out] table
 * @param[out] transaction
 *
 * @return status
 */

RS_Status PKROperation::setupTransaction() {
  transaction = ndbObject->startTransaction(tableDic);
  if (transaction == nullptr) {
    return RS_ERROR(ndbObject->getNdbError(), ERROR_005);
  }
  return RS_OK;
}

/**
 * Set up read operation
 *
 * @param[in] ndbObject
 * @param[in] table
 * @param[in] transaction
 * @param[out] operation
 *
 * @return status
 */
RS_Status PKROperation::setupReadOperation() {

  if (operation != NULL) {
    return RS_ERROR(ERROR_006);
  }

  operation = transaction->getNdbOperation(tableDic);
  if (operation == nullptr) {
    return RS_ERROR(transaction->getNdbError(), ERROR_007);
  }

  operation->readTuple(NdbOperation::LM_CommittedRead);

  for (uint32_t i = 0; i < request.pkColumnsCount(); i++) {
    RS_Status status = setOperationPKCols(tableDic->getColumn(request.pkName(i)), i);
    if (status.http_code != SUCCESS) {
      return status;
    }
  }

  if (request.readColumnsCount() > 0) {
    for (uint32_t i = 0; i < request.readColumnsCount(); i++) {
      NdbRecAttr *rec = operation->getValue(request.readColumnName(i), NULL);
      recs.insert(recs.begin(), rec);
    }
  } else {
    std::unordered_map<std::string, const NdbDictionary::Column *>::const_iterator it =
        nonPkCols.begin();
    while (it != nonPkCols.end()) {
      NdbRecAttr *rec = operation->getValue(it->first.c_str(), NULL);
      it++;
      recs.insert(recs.begin(), rec);
    }
  }

  return RS_OK;
}

RS_Status PKROperation::execute() {
  if (transaction->execute(NdbTransaction::Commit) != 0) {
    return RS_ERROR(transaction->getNdbError(), ERROR_009);
  }

  return RS_OK;
}

RS_Status PKROperation::createResponse() {
  if (transaction->getNdbError().classification == NdbError::NoDataFound) {
    char message[] = "NOT FOUND";
    memcpy(response.getResponseBuffer(), message, sizeof(message));
    response.getResponseBuffer()[strlen(message)] = 0x00;
  } else {

    // iterate over all columns
    response.appendStr("{", false);
    if (request.operationId() != NULL) {
      response.appendStr("\"OperationID\": ", false);
      response.appendStr(string("\"") + request.operationId() + string("\""), true);
    }
    response.appendStr("\"Data\": {", false);

    for (uint32_t i = 0; i < recs.size(); i++) {

      RS_Status status =
          response.appendStr(string("\"") + recs[i]->getColumn()->getName() + string("\":"), false);
      if (status.http_code != SUCCESS) {
        return status;
      }

      status = writeColToRespBuff(recs[i], i == (recs.size() - 1) ? false : true);
      if (status.http_code != SUCCESS) {
        return status;
      }
    }
    response.appendStr("} } ", false);
    response.appendNULL();
  }

  return RS_OK;
}

int PKROperation::get_byte_array(const NdbRecAttr *attr, const char *&first_byte, int *bytes) {

  const NdbDictionary::Column::ArrayType array_type = attr->getColumn()->getArrayType();
  const size_t attr_bytes                           = attr->get_size_in_bytes();
  const char *aRef                                  = attr->aRef();
  string result;

  switch (array_type) {
  case NdbDictionary::Column::ArrayTypeFixed:
    /*
     No prefix length is stored in aRef. Data starts from aRef's first byte
     data might be padded with blank or null bytes to fill the whole column
     */
    first_byte = aRef;
    *bytes     = attr_bytes;
    return 0;
  case NdbDictionary::Column::ArrayTypeShortVar:
    /*
     First byte of aRef has the length of data stored
     Data starts from second byte of aRef
     */
    first_byte = aRef + 1;
    *bytes     = (size_t)(aRef[0]);
    return 0;
  case NdbDictionary::Column::ArrayTypeMediumVar:
    /*
     First two bytes of aRef has the length of data stored
     Data starts from third byte of aRef
     */
    first_byte = aRef + 2;
    *bytes     = (size_t)(aRef[1]) * 256 + (size_t)(aRef[0]);
    cout << "Data length " << *bytes << endl;
    return 0;
  default:
    first_byte = NULL;
    *bytes     = 0;
    return -1;
  }
}

// https://docs.oracle.com/cd/E17952_01/ndbapi-en/ndbapi-examples-array-simple.html
int PKROperation::copyString(const NdbRecAttr *attr, int start) {
  int attr_bytes;
  const char *data_start_ptr = NULL;

  /* get stored length and data using get_byte_array */
  if (get_byte_array(attr, data_start_ptr, &attr_bytes) == 0) {
    memcpy(response.getResponseBuffer() + start, data_start_ptr, attr_bytes);

    string str = string(data_start_ptr, attr_bytes);
    /* sprintf(NULL,"PTR: %p\n", data_start_ptr); */
    /* we have length of the string and start location */
    //    str = string(data_start_ptr, attr_bytes);
    //    if (attr->getType() == NdbDictionary::Column::Char) {
    //      /* Fixed Char : remove blank spaces at the end */
    //      size_t endpos = str.find_last_not_of(" ");
    //      if (string::npos != endpos) {
    //        str = str.substr(0, endpos + 1);
    //      }
    //    }
    response.getResponseBuffer()[start + attr_bytes] = 0x00;
    return start + attr_bytes + 1;
  }
  return -1;
}

RS_Status PKROperation::init() {
  if (tableDic == NULL) {
    if (ndbObject->setCatalogName(request.db()) != 0) {
      return RS_ERROR(ERROR_011 + string(" Database: ") + string(request.db()) +
                      " Table: " + request.table());
    }
    const NdbDictionary::Dictionary *dict = ndbObject->getDictionary();
    tableDic                              = dict->getTable(request.table());

    if (tableDic == nullptr) {
      return RS_ERROR(ERROR_011 + string(" Database: ") + string(request.db()) +
                      " Table: " + request.table());
    }
  }

  // get all primary key columnns
  for (int i = 0; i < tableDic->getNoOfPrimaryKeys(); i++) {
    const char *priName     = tableDic->getPrimaryKey(i);
    pkCols[string(priName)] = tableDic->getColumn(priName);
  }

  // get all non primary key columnns
  for (int i = 0; i < tableDic->getNoOfColumns(); i++) {
    const NdbDictionary::Column *col = tableDic->getColumn(i);
    string colNameStr(col->getName());
    std::unordered_map<std::string, const NdbDictionary::Column *>::const_iterator got =
        pkCols.find(colNameStr);
    if (got == pkCols.end()) { // not found
      nonPkCols[string(col->getName())] = tableDic->getColumn(col->getName());
    }
  }

  return RS_OK;
}

RS_Status PKROperation::validateRequest() {

  // Check primary key columns
  if (request.pkColumnsCount() != pkCols.size()) {
    return RS_ERROR(ERROR_013 + string(" Expecting: ") + to_string(pkCols.size()) +
                    " Got: " + to_string(request.pkColumnsCount()));
  }

  for (uint32_t i = 0; i < request.pkColumnsCount(); i++) {
    std::unordered_map<std::string, const NdbDictionary::Column *>::const_iterator got =
        pkCols.find(string(request.pkName(i)));
    if (got == pkCols.end()) { // not found
      return RS_ERROR(ERROR_014 + string(" Column: ") + string(request.pkName(i)));
    }
  }

  // Check non primary key columns
  // check that all columns exist
  if (request.readColumnsCount() > 0) {
    for (uint32_t i = 0; i < request.readColumnsCount(); i++) {
      std::unordered_map<std::string, const NdbDictionary::Column *>::const_iterator got =
          nonPkCols.find(string(request.readColumnName(i)));
      if (got == nonPkCols.end()) { // not found
        return RS_ERROR(ERROR_012 + string(" Column: ") + string(request.readColumnName(i)));
      }
    }
  }

  return RS_OK;
}

void PKROperation::closeTransaction() {
  ndbObject->closeTransaction(transaction);
}

RS_Status PKROperation::performOperation() {
  RS_Status status = init();
  if (status.http_code != SUCCESS) {
    return status;
  }

  status = validateRequest();
  if (status.http_code != SUCCESS) {
    return status;
  }

  status = setupTransaction();
  if (status.http_code != SUCCESS) {
    abort();
    return status;
  }

  status = setupReadOperation();
  if (status.http_code != SUCCESS) {
    abort();
    return status;
  }

  status = execute();
  if (status.http_code != SUCCESS) {
    abort();
    return status;
  }

  status = createResponse();
  if (status.http_code != SUCCESS) {
    abort();
    return status;
  }

  closeTransaction();
  return RS_OK;
}

RS_Status PKROperation::abort() {
  if (transaction != NULL) {
    NdbTransaction::CommitStatusType status = transaction->commitStatus();
    if (status == NdbTransaction::CommitStatusType::Started) {
      transaction->execute(NdbTransaction::Rollback);
    }
    ndbObject->closeTransaction(transaction);
  }

  return RS_OK;
}

RS_Status PKROperation::writeColToRespBuff(const NdbRecAttr *attr, bool appendComma) {
  const NdbDictionary::Column *col = attr->getColumn();
  RS_Status status;

  if (attr->isNULL()) {
    return response.appendStr("null", appendComma);
  }

  switch (col->getType()) {
  case NdbDictionary::Column::Undefined: {
    ///< 4 bytes + 0-3 fraction
    TRACE(string("Getting PK Column: ") + string(col->getName()) + " Type: Undefined");
    return RS_OK;
  }
  case NdbDictionary::Column::Tinyint: {
    ///< 8 bit. 1 byte signed integer, can be used in array
    status = response.append_i8(attr->int8_value(), appendComma);
    break;
  }
  case NdbDictionary::Column::Tinyunsigned: {
    ///< 8 bit. 1 byte unsigned integer, can be used in array
    status = response.append_iu8(attr->u_8_value(), appendComma);
    break;
  }
  case NdbDictionary::Column::Smallint: {
    ///< 16 bit. 2 byte signed integer, can be used in array
    status = response.append_i16(attr->short_value(), appendComma);
    break;
  }
  case NdbDictionary::Column::Smallunsigned: {
    ///< 16 bit. 2 byte unsigned integer, can be used in array
    status = response.append_iu16(attr->u_short_value(), appendComma);
    break;
  }
  case NdbDictionary::Column::Mediumint: {
    ///< 24 bit. 3 byte signed integer, can be used in array
    TRACE(string("Getting PK Column: ") + string(col->getName()) + " Type: Mediumint")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Mediumunsigned: {
    ///< 24 bit. 3 byte unsigned integer, can be used in array
    TRACE(string("Getting PK Column: ") + string(col->getName()) + " Type: Mediumunsigned")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Int: {
    ///< 32 bit. 4 byte signed integer, can be used in array
    status = response.append_i32(attr->int32_value(), appendComma);
    break;
  }
  case NdbDictionary::Column::Unsigned: {
    ///< 32 bit. 4 byte unsigned integer, can be used in array
    status = response.append_iu32(attr->u_32_value(), appendComma);
    break;
  }
  case NdbDictionary::Column::Bigint: {
    ///< 64 bit. 8 byte signed integer, can be used in array
    status = response.append_i64(attr->int64_value(), appendComma);
    break;
  }
  case NdbDictionary::Column::Bigunsigned: {
    ///< 64 Bit. 8 byte signed integer, can be used in array
    status = response.append_iu64(attr->u_64_value(), appendComma);
    break;
  }
  case NdbDictionary::Column::Float: {
    ///< 32-bit float. 4 bytes float, can be used in array
    TRACE(string("Getting PK Column: ") + string(col->getName()) + " Type: Float")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Double: {
    ///< 64-bit float. 8 byte float, can be used in array
    TRACE(string("Getting PK Column: ") + string(col->getName()) + " Type: Double")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Olddecimal: {
    ///< MySQL < 5.0 signed decimal,  Precision, Scale
    TRACE(string("Getting PK Column: ") + string(col->getName()) + " Type: Olddecimal")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Olddecimalunsigned: {
    TRACE(string("Getting PK Column: ") + string(col->getName()) + " Type: Olddecimalunsigned")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Decimal: {
    ///< MySQL >= 5.0 signed decimal,  Precision, Scale
    TRACE(string("Getting PK Column: ") + string(col->getName()) + " Type: Decimal")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Decimalunsigned: {
    TRACE(string("Getting PK Column: ") + string(col->getName()) + " Type: Decimalunsigned")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Char: {
    ///< Len. A fixed array of 1-byte chars
    TRACE(string("Getting PK Column: ") + string(col->getName()) + " Type: Char")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Varchar: {
    ///< Length bytes: 1, Max: 255
    TRACE(string("Getting PK Column: ") + string(col->getName()) + " Type: Varchar")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Binary: {
    ///< Len
    TRACE(string("Getting PK Column: ") + string(col->getName()) + " Type: Binary")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Varbinary: {
    ///< Length bytes: 1, Max: 255
    TRACE(string("Getting PK Column: ") + string(col->getName()) + " Type: Varbinary")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Datetime: {
    ///< Precision down to 1 sec (sizeof(Datetime) == 8 bytes )
    TRACE(string("Getting PK Column: ") + string(col->getName()) + " Type: Datetime")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Date: {
    ///< Precision down to 1 day(sizeof(Date) == 4 bytes )
    TRACE(string("Getting PK Column: ") + string(col->getName()) + " Type: Date")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Blob: {
    ///< Binary large object (see NdbBlob)
    TRACE(string("Getting PK Column: ") + string(col->getName()) + " Type: Blob")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Text: {
    ///< Text blob
    TRACE(string("Getting PK Column: ") + string(col->getName()) + " Type: Text")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Bit: {
    ///< Bit, length specifies no of bits
    TRACE(string("Getting PK Column: ") + string(col->getName()) + " Type: Bit")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Longvarchar: {
    ///< Length bytes: 2, little-endian
    TRACE(string("Getting PK Column: ") + string(col->getName()) + " Type: Longvarchar")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Longvarbinary: {
    ///< Length bytes: 2, little-endian
    TRACE(string("Getting PK Column: ") + string(col->getName()) + " Type: Longvarbinary")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Time: {
    ///< Time without date
    TRACE(string("Getting PK Column: ") + string(col->getName()) + " Type: Time")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Year: {
    ///< Year 1901-2155 (1 byte)
    TRACE(string("Getting PK Column: ") + string(col->getName()) + " Type: Year")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Timestamp: {
    ///< Unix time
    TRACE(string("Getting PK Column: ") + string(col->getName()) + " Type: Timestamp")
    return RS_ERROR("Not Implemented");
  }
  ///**
  // * Time types in MySQL 5.6 add microsecond fraction.
  // * One should use setPrecision(x) to set number of fractional
  // * digits (x = 0-6, default 0).  Data formats are as in MySQL
  // * and must use correct byte length.  NDB does not check data
  // * itself since any values can be compared as binary strings.
  // */
  case NdbDictionary::Column::Time2: {
    ///< 3 bytes + 0-3 fraction
    TRACE(string("Getting PK Column: ") + string(col->getName()) + " Type: Time2")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Datetime2: {
    ///< 5 bytes plus 0-3 fraction
    TRACE(string("Getting PK Column: ") + string(col->getName()) + " Type: Datetime2")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Timestamp2: {
    ///< 4 bytes + 0-3 fraction
    TRACE(string("Getting PK Column: ") + string(col->getName()) + " Type: Timestamp2");
    return RS_ERROR("Not Implemented");
  }
  }

  if (status.http_code != SUCCESS) {
    return status;
  } else {
    return RS_OK;
  }
}

RS_Status PKROperation::setOperationPKCols(const NdbDictionary::Column *col, uint32_t colIdx) {
  // validate the data and set data according to column type
  char *data;

  switch (col->getType()) {
  case NdbDictionary::Column::Undefined: {
    ///< 4 bytes + 0-3 fraction
    TRACE(string("Setting PK Column: ") + string(col->getName()) + " Type: Undefined");
    return RS_OK;
  }
  case NdbDictionary::Column::Tinyint: {
    ///< 8 bit. 1 byte signed integer, can be used in array
    bool success = false;
    try {
      int num = stoi(request.pkValueCStr(colIdx));
      if (num >= -128 && num <= 127) {
        operation->equal(request.pkName(colIdx), (char)num);
        success = true;
      }
    } catch (...) {
    }
    if (!success) {
      return RS_ERROR(ERROR_015 + string(" Expecting TINYINT. Column: ") +
                      string(request.pkName(colIdx)));
    } else {
      return RS_OK;
    }
  }
  case NdbDictionary::Column::Tinyunsigned: {
    ///< 8 bit. 1 byte unsigned integer, can be used in array
    bool success = false;
    try {
      int num = stoi(request.pkValueCStr(colIdx));
      if (num >= 0 && num <= 255) {
        operation->equal(request.pkName(colIdx), (char)num);
        success = true;
      }
    } catch (...) {
    }
    if (!success) {
      return RS_ERROR(ERROR_015 + string(" Expecting TINYINT. Column: ") +
                      string(request.pkName(colIdx)));
    } else {
      return RS_OK;
    }
  }
  case NdbDictionary::Column::Smallint: {
    ///< 16 bit. 2 byte signed integer, can be used in array
    bool success = false;
    try {
      int num = stoi(request.pkValueCStr(colIdx));
      if (num >= -32768 && num <= 32767) {
        operation->equal(request.pkName(colIdx), (short int)num);
        success = true;
      }
    } catch (...) {
    }
    if (!success) {
      return RS_ERROR(ERROR_015 + string(" Expecting SMALLINT. Column: ") +
                      string(request.pkName(colIdx)));
    } else {
      return RS_OK;
    }
  }
  case NdbDictionary::Column::Smallunsigned: {
    ///< 16 bit. 2 byte unsigned integer, can be used in array
    bool success = false;
    try {
      int num = stoi(request.pkValueCStr(colIdx));
      if (num >= 0 && num <= 65535) {
        operation->equal(request.pkName(colIdx), (unsigned short int)num);
        success = true;
      }
    } catch (...) {
    }
    if (!success) {
      return RS_ERROR(ERROR_015 + string(" Expecting TINYINT. Column: ") +
                      string(request.pkName(colIdx)));
    } else {
      return RS_OK;
    }
  }
  case NdbDictionary::Column::Mediumint: {
    ///< 24 bit. 3 byte signed integer, can be used in array
    TRACE(string("Setting PK Column: ") + string(col->getName()) + " Type: Mediumint")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Mediumunsigned: {
    ///< 24 bit. 3 byte unsigned integer, can be used in array
    TRACE(string("Setting PK Column: ") + string(col->getName()) + " Type: Mediumunsigned")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Int: {
    ///< 32 bit. 4 byte signed integer, can be used in array
    try {
      int num = stoi(request.pkValueCStr(colIdx));
      operation->equal(request.pkName(colIdx), num);
    } catch (...) {
      return RS_ERROR(ERROR_015 + string(" Expecting Int. Column: ") +
                      string(request.pkName(colIdx)));
    }
    return RS_OK;
  }
  case NdbDictionary::Column::Unsigned: {
    ///< 32 bit. 4 byte unsigned integer, can be used in array
    bool success = false;
    try {
      long long lresult   = stoll(request.pkValueCStr(colIdx));
      unsigned int result = lresult;
      if (result == lresult) {
        operation->equal(request.pkName(colIdx), result);
        success = true;
      }
    } catch (...) {
    }

    if (!success) {
      return RS_ERROR(ERROR_015 + string(" Expecting Unsigned Int. Column: ") +
                      string(request.pkName(colIdx)));
    } else {
      return RS_OK;
    }
  }
  case NdbDictionary::Column::Bigint: {
    ///< 64 bit. 8 byte signed integer, can be used in array
    try {
      long long num = stoll(request.pkValueCStr(colIdx));
      operation->equal(request.pkName(colIdx), num);
      cout << "Setting big int to " << num << endl;
    } catch (...) {
      return RS_ERROR(ERROR_015 + string(" Expecting BIGINT. Column: ") +
                      string(request.pkName(colIdx)));
    }
    return RS_OK;
  }
  case NdbDictionary::Column::Bigunsigned: {
    ///< 64 Bit. 8 byte signed integer, can be used in array
    bool success = false;
    try {
      const char *numCStr = request.pkValueCStr(colIdx);
      const string numStr = string(numCStr);
      if (numStr.find('-') == string::npos) {
        unsigned long long num = stoul(numCStr);
        operation->equal(request.pkName(colIdx), num);
        success = true;
      }
    } catch (...) {
    }
    if (!success) {
      return RS_ERROR(ERROR_015 + string(" Expecting BIGINT UNSIGNED. Column: ") +
                      string(request.pkName(colIdx)));
    } else {
      return RS_OK;
    }
  }
  case NdbDictionary::Column::Float: {
    ///< 32-bit float. 4 bytes float, can be used in array
    TRACE(string("Setting PK Column: ") + string(col->getName()) + " Type: Float")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Double: {
    ///< 64-bit float. 8 byte float, can be used in array
    TRACE(string("Setting PK Column: ") + string(col->getName()) + " Type: Double")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Olddecimal: {
    ///< MySQL < 5.0 signed decimal,  Precision, Scale
    TRACE(string("Setting PK Column: ") + string(col->getName()) + " Type: Olddecimal")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Olddecimalunsigned: {
    TRACE(string("Setting PK Column: ") + string(col->getName()) + " Type: Olddecimalunsigned")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Decimal: {
    ///< MySQL >= 5.0 signed decimal,  Precision, Scale
    TRACE(string("Setting PK Column: ") + string(col->getName()) + " Type: Decimal")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Decimalunsigned: {
    TRACE(string("Setting PK Column: ") + string(col->getName()) + " Type: Decimalunsigned")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Char: {
    ///< Len. A fixed array of 1-byte chars
    TRACE(string("Setting PK Column: ") + string(col->getName()) + " Type: Char")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Varchar: {
    ///< Length bytes: 1, Max: 255
    TRACE(string("Setting PK Column: ") + string(col->getName()) + " Type: Varchar")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Binary: {
    ///< Len
    TRACE(string("Setting PK Column: ") + string(col->getName()) + " Type: Binary")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Varbinary: {
    ///< Length bytes: 1, Max: 255
    TRACE(string("Setting PK Column: ") + string(col->getName()) + " Type: Varbinary")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Datetime: {
    ///< Precision down to 1 sec (sizeof(Datetime) == 8 bytes )
    TRACE(string("Setting PK Column: ") + string(col->getName()) + " Type: Datetime")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Date: {
    ///< Precision down to 1 day(sizeof(Date) == 4 bytes )
    TRACE(string("Setting PK Column: ") + string(col->getName()) + " Type: Date")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Blob: {
    ///< Binary large object (see NdbBlob)
    TRACE(string("Setting PK Column: ") + string(col->getName()) + " Type: Blob")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Text: {
    ///< Text blob
    TRACE(string("Setting PK Column: ") + string(col->getName()) + " Type: Text")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Bit: {
    ///< Bit, length specifies no of bits
    TRACE(string("Setting PK Column: ") + string(col->getName()) + " Type: Bit")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Longvarchar: {
    ///< Length bytes: 2, little-endian
    TRACE(string("Setting PK Column: ") + string(col->getName()) + " Type: Longvarchar")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Longvarbinary: {
    ///< Length bytes: 2, little-endian
    TRACE(string("Setting PK Column: ") + string(col->getName()) + " Type: Longvarbinary")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Time: {
    ///< Time without date
    TRACE(string("Setting PK Column: ") + string(col->getName()) + " Type: Time")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Year: {
    ///< Year 1901-2155 (1 byte)
    TRACE(string("Setting PK Column: ") + string(col->getName()) + " Type: Year")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Timestamp: {
    ///< Unix time
    TRACE(string("Setting PK Column: ") + string(col->getName()) + " Type: Timestamp")
    return RS_ERROR("Not Implemented");
  }
  ///**
  // * Time types in MySQL 5.6 add microsecond fraction.
  // * One should use setPrecision(x) to set number of fractional
  // * digits (x = 0-6, default 0).  Data formats are as in MySQL
  // * and must use correct byte length.  NDB does not check data
  // * itself since any values can be compared as binary strings.
  // */
  case NdbDictionary::Column::Time2: {
    ///< 3 bytes + 0-3 fraction
    TRACE(string("Setting PK Column: ") + string(col->getName()) + " Type: Time2")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Datetime2: {
    ///< 5 bytes plus 0-3 fraction
    TRACE(string("Setting PK Column: ") + string(col->getName()) + " Type: Datetime2")
    return RS_ERROR("Not Implemented");
  }
  case NdbDictionary::Column::Timestamp2: {
    ///< 4 bytes + 0-3 fraction
    TRACE(string("Setting PK Column: ") + string(col->getName()) + " Type: Timestamp2");
    return RS_ERROR("Not Implemented");
  }
  }

  /* if (request.pkValueNDBStr(i, tableDic->getColumn(request.pkName(i)), &data) != 0)  */
  /* operation->equal(request.pkName(i), data); */
  return RS_OK;
}
