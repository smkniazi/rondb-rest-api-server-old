# RonDB REST API Server 

Currently, the REST API server only supports (non-)batched  primary key operations. Default mapping of MySQL data types to JSON data types are as follows


| MySQL Data Type | JSON Data Type |
| --------------- | -------------- |
| TINYINT, SMALLINT MEDIUMINT, INT, BIGINT  | number |
| FLOAT, DOUBLE, DECIMAL  | number |
| CHAR, VARCHAR  | escaped string |
| BINARY, VARBINARY  | base64 encoded string |
| DATE, DATETIME, TIME, TIMESTAMP, YEAR   | string |
| YEAR   | number |
| BIT    | base64 encoded string |



## POST /0.1.0/{database}/{table}/pk-read

Is used to perform a primary key read operation. 

Assume we have the following table.

```
CREATE TABLE `my_table` (                                            
  `id0` int NOT NULL,                                                 
  `id1` int unsigned NOT NULL,                                        
  `col0` int DEFAULT NULL,                                            
  `col1` int unsigned DEFAULT NULL,                                   
  PRIMARY KEY (`id0`,`id1`)                                           
) ENGINE=ndbcluster DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
```

**Path Parameters:**

  - *api-version* : current api version is 0.1.0
  - *database* : database name
  - *table* : table name

**Body:**

```json
{
  "filters": [
    {
      "column": "id0",
      "value": 0
    },
    {
      "column": "id1",
      "value": 0
    }
  ],
  "readColumns": [
    {
      "column": "col0",
      "dataReturnType": "default"
    },
    {
      "column": "col1",
      "dataReturnType": "default"
    }
  ],
  "operationId": "ABC123"
}

```

  - **filters** : This is mandatory parameter. It is an array of objects one for each column that forms the primary key. 
  - **readColumns** : It is an optional parameter that is used to perform projections. If it is omitted then all the columns of the table will be read
    - **dataReturnType** : It is an optional parameter. It can be used to control in which format the data is returned, for example, hex, base64, etc. However, in this version (0.1.0) we only support the default return type.  
  - **operationId** : It is an optional parameter. It is a *string* parameter and it can be up to 64 characters long. 

**Response**

```json
{
  "operationId": "ABC123",
  "data": {
    "col0": 123,
    "col1": 456
  }
}
```

## POST /0.1.0/batch

Is used to perform batched primary key read operations. 

**Path Parameters:**

  - *api-version* : current api version is 0.1.0

**Body:**

```json
{
  "operations": [
    {
      "method": "POST",
      "relative-url": "my_database_1/my_table_1/pk-read",
      "body": {
        "filters": [
          {
            "column": "id0",
            "value": 0
          },
          {
            "column": "id1",
            "value": 0
          }
        ],
        "readColumns": [
          {
            "column": "col0",
            "dataReturnType": "default"
          },
          {
            "column": "col1",
            "dataReturnType": "default"
          }
        ],
        "operationId": "1"
      },
    },

    {
      "method": "POST",
      "relative-url": "my_database_2/my_table_2/pk-read",
      "body": {
        "filters": [
          {
            "column": "id0",
            "value": 1
          },
          {
            "column": "id1",
            "value": 1
          }
        ],
      },
    },
  ]
}
```

**Response**

```json
[
  {
    "code": 200,
    "body": {
      "operationId": "1",
      "data": {
        "col0": 0,
        "col1": 0
      }
    }
  },
  {
    "code": 200,
    "body": {
      "data": {
        "col0": 1,
        "col1": 1
      }
    }
  }
]
```
