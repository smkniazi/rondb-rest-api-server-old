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
    char *data;
    if (request.pkValueNDBStr(i, tableDic->getColumn(request.pkName(i)), &data) != 0) {
      return RS_ERROR(ERROR_008 + string(" Column: ") + string(request.pkName(i)));
    }
    operation->equal(request.pkName(i), data);
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
    int head = 0;
    for (std::vector<NdbRecAttr *>::iterator it = std::begin(recs); it != std::end(recs); ++it) {
      head = copyString(*it, head);
      if (head == -1) {
        return RS_ERROR(ERROR_010);
      }
    }
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
  // TODO check pk col data type

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

  // check data types
  // TODO

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
    return status;
  }

  status = setupReadOperation();
  if (status.http_code != SUCCESS) {
    return status;
  }

  status = execute();
  if (status.http_code != SUCCESS) {
    return status;
  }

  status = createResponse();
  if (status.http_code != SUCCESS) {
    return status;
  }

  closeTransaction();
  return RS_OK;
}
