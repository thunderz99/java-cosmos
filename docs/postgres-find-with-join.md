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

## sample SQL 1 

for returnAllSubArray=false

```java
var cond = Condition.filter(
          "area.city.street.rooms.no", "001",
          "room*no-01.area >",  3
          )
          .join(Set.of("area.city.street.rooms", "room*no-01"))
          .returnAllSubArray(false)
          ;
```

```SQL
SELECT 
    id,
    jsonb_set(
      jsonb_set(
        data,
        '{area,city,street,rooms}',
        COALESCE(
        (
          SELECT jsonb_agg(s0)
          FROM jsonb_array_elements(data->'area'->'city'->'street'->'rooms') s0
          WHERE (s0->>'no' = @param000_no__for_select)
        ),
        data->'area'->'city'->'street'->'rooms'
        )
      ),
      '{room*no-01}',
      COALESCE(
      (
        SELECT jsonb_agg(s1)
        FROM jsonb_array_elements(data->'room*no-01') s1
        WHERE ((s1->>'area')::int > @param001_area__for_select)
      ),
      data->'room*no-01'
    )) AS data
FROM schema1.families
WHERE EXISTS (
  SELECT 1
  FROM jsonb_array_elements(data->'area'->'city'->'street'->'rooms') AS j0
  WHERE (j0->>'no' = @param000_no)
)
AND EXISTS (
  SELECT 1
  FROM jsonb_array_elements(data->'room*no-01') AS j1
  WHERE ((j1->>'area')::int > @param001_area)
);
```



## sample SQL 2

for returnAllSubArray=false, and both "name" and "no" in the same joinBaseKey("floors")

```java

var cond = Condition.filter(
          "floors.rooms ARRAY_CONTAINS_ANY name",  List.of("r1", "r2"),
          "floors.rooms ARRAY_CONTAINS_ANY no",  List.of("001", "002")
          )
          .join(Set.of("floors"))
          .returnAllSubArray(false)
          ;
```


```SQL
SELECT id, jsonb_set(
      data,
      '{"floors"}',
      COALESCE(
         (
       SELECT jsonb_agg(s1)
       FROM jsonb_array_elements(data->'floors') AS s1
       WHERE (s1->'rooms' @> jsonb_build_array(jsonb_build_object('name', @param001_rooms__name__0__for_select)) OR s1->'rooms' @> jsonb_build_array(jsonb_build_object('name', @param001_rooms__name__1__for_select)))
       AND (s1->'rooms' @> jsonb_build_array(jsonb_build_object('name', @param003_rooms__no__0__for_select)) OR s1->'rooms' @> jsonb_build_array(jsonb_build_object('name', @param003_rooms__name__1__for_select)))
     ),
        data->'floors'
      )
    )
     AS data
     FROM schema1.table1
     WHERE EXISTS (
        SELECT 1
        FROM jsonb_array_elements(data->'floors') AS j0
        WHERE (j0->'rooms' @> jsonb_build_array(jsonb_build_object('name', @param000_rooms__name__0)) OR j0->'rooms' @> jsonb_build_array(jsonb_build_object('name', @param000_rooms__name__1)))
      ) AND EXISTS (
        SELECT 1
        FROM jsonb_array_elements(data->'floors') AS j2
        WHERE (j2->'rooms' @> jsonb_build_array(jsonb_build_object('no', @param002_rooms__no__0)) OR j2->'rooms' @> jsonb_build_array(jsonb_build_object('no', @param002_rooms__no__1)))
      ) OFFSET 0 LIMIT 100"
```



### sample SQL 3



with fields



```java
var cond = Condition.filter(
          "area.city.street.rooms.no", "001",
          "room*no-01.area >",  3
          )
          .join(Set.of("area.city.street.rooms", "room*no-01"))
          .returnAllSubArray(false)
  				.fields("id", "address", "room*no-01")
          ;

```



```SQL
WITH filtered_data AS (
  SELECT 
    id,
    jsonb_set(
      jsonb_set(
        data,
        '{area,city,street,rooms}',
        COALESCE(
          (
            SELECT jsonb_agg(s0)
            FROM jsonb_array_elements(data->'area'->'city'->'street'->'rooms') s0
            WHERE s0->>'no' = '001'
          ),
          data->'area'->'city'->'street'->'rooms'
        )
      ),
      '{room*no-01}',
      COALESCE(
        (
          SELECT jsonb_agg(s1)
          FROM jsonb_array_elements(data->'room*no-01') s1
          WHERE (s1->>'area')::int > 10
        ),
        data->'room*no-01'
      )
    ) AS data
  FROM unittest_postgres_5bwo.families
  WHERE EXISTS (
    SELECT 1
    FROM jsonb_array_elements(data->'area'->'city'->'street'->'rooms') j0
    WHERE j0->>'no' = '001'
  )
  AND EXISTS (
    SELECT 1
    FROM jsonb_array_elements(data->'room*no-01') j1
    WHERE (j1->>'area')::int > 10
  )
)
SELECT 
  id, 
  jsonb_build_object(
    'id', data->'id',
    'address', data->'address',
	'room*no-01',data->'room*no-01'
  ) AS data
FROM filtered_data
 ORDER BY data->'id' ASC, data->'_ts' ASC
 OFFSET 0 LIMIT 100
```





## implementation

The above algorithm is implemented by `PGSubQueryExpression4JsonPath`



## reference

* sample SQL 1
  * https://chatgpt.com/share/679b3748-a768-8007-b8bc-68d57487066e

* sample SQL 3
  * https://chatgpt.com/share/67a1d39a-8d14-8007-9117-33c40709ad90