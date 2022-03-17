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
#include "src/logger.hpp"
#include "src/rdrs-const.h"
#include "src/status.hpp"

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

  if (ndbObject->setCatalogName(request.db()) != 0) {
    return RS_ERROR(1, "Database does not exist. Database: " + string(request.db()));
  }

  const NdbDictionary::Dictionary *dict = ndbObject->getDictionary();
  tableDic                              = dict->getTable(request.table());

  if (tableDic == nullptr) {
    return RS_ERROR(1, "Table does not exist. Table: " + string(request.table()));
  }

  transaction = ndbObject->startTransaction(tableDic);
  if (transaction == nullptr) {
    return RS_ERROR(ndbObject->getNdbError(), "Failed to start transaction.");
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
  operation = transaction->getNdbOperation(tableDic);
  if (operation == nullptr) {
    return RS_ERROR(transaction->getNdbError(), "Failed to start read operation.");
  }

  operation->readTuple(NdbOperation::LM_CommittedRead);
  for (uint32_t i = 0; i < request.pkColumnsCount(); i++) {

    char *data;
    if (request.pkValueNDBStr(i, tableDic->getColumn(request.pkName(i)), &data) != 0) {
      return RS_ERROR(-1, "Invalid data for column \"" + string(request.pkName(i)) + "\"");
    }
    operation->equal(request.pkName(i), data);
    // operation->equal(pkName(i),  pkValue(i));
  }

  rec = operation->getValue("value", NULL);
  if (rec == nullptr) {
    return RS_ERROR(operation->getNdbError(), "Failed to create operation.");
  }

  return RS_OK;
}

RS_Status PKROperation::execute() {
  if (transaction->execute(NdbTransaction::Commit) != 0) {
    return RS_ERROR(transaction->getNdbError(), "Failed to execute transaction");
  }

  return RS_OK;
}

RS_Status PKROperation::createResponse() {
  if (transaction->getNdbError().classification == NdbError::NoDataFound) {
    char message[] = "NOT FOUND";
    memcpy(response.getResponseBuffer(), message, sizeof(message));
    response.getResponseBuffer()[strlen(message)] = 0x00;
  } else {
    copyString(rec, 0);
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
  return 0;
}

void PKROperation::closeTransaction() {
  ndbObject->closeTransaction(transaction);
}

RS_Status PKROperation::performOperation() {

  RS_Status status = setupTransaction();
  if (status.ret_code != 0) {
    return status;
  }

  status = setupReadOperation();
  if (status.ret_code != 0) {
    return status;
  }

  status = execute();
  if (status.ret_code != 0) {
    return status;
  }

  status = createResponse();
  if (status.ret_code != 0) {
    return status;
  }

  closeTransaction();
  return RS_OK;
}
