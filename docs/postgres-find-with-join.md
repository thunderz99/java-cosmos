# Find with join


## introduction

Finding documents when JOIN is used and `returnAllSubArray = false`.
In postgres this is implemented by using the same filter both in the SELECT clause and the WHERE clause.

Note: When `returnAllSubArray = true`, things are simple and we can do this by a normal postgres find, without filtering the result in the SELECT clause. Because the original documents are remained unmodified.

## sample documents in db
```json
{
  "_id": "AndersenFamily",
  "data": 
  {
    "id": "AndersenFamily",
    "lastName": "Andersen",
    "parents": [
      {
        "firstName": "Thomas"
      },
      {
        "firstName": "Mary Kay"
      }
    ],
    "children": [
      {
        "firstName": "Henriette Thaulow",
        "gender": "female",
        "grade": 5,
        "pets": [
          {
            "givenName": "Fluffy"
          }
        ]
      }
    ],
    "room*no-01": [
      {
        "area": 10
      },
      {
        "area": 12
      }
    ],
    "area": {
      "city": {
        "street": {
          "name": "tokyo street",
          "rooms": [
            {
              "no": "001",
              "floor": 3
            },
            {
              "no": "002",
              "floor": 1
            }
          ]
        }
      }
    },
    "address": {
      "state": "WA",
      "county": "King",
      "city": "Seattle"
    },
    "creationDate": 1431620472,
    "isRegistered": true,
    "_partition": "Families"
  }
}

```

## sample SQL



```SQL
SELECT 
  id,
  jsonb_set(
    jsonb_set(
      data,
      '{area,city,street,rooms}',
      jsonb_path_query_array(data, $1::jsonpath)  -- same filter as WHERE
    ),
    '{room*no-01}',
    jsonb_path_query_array(data, $2::jsonpath)    -- same filter as WHERE
  ) AS "data"
FROM schema1.families
WHERE data @? $1::jsonpath
  AND data @? $2::jsonpath;
```

## implementation

The above algorithm is implemented by `PGSubQueryExpression4JsonPath`



## reference

https://chatgpt.com/share/679b3748-a768-8007-b8bc-68d57487066e