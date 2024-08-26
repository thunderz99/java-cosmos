# Find with join


## introduction

Finding documents when JOIN is used and `returnAllSubArray = false`.
In mongo this is implemented by aggregate pipeline and using $project stage and $filter.

Note: When `returnAllSubArray = true`, things are simple and we can do this by a normal mongo find, without using aggregate pipeline. Because the original documents are remained unmodified.

## sample documents in db
```json
[
  {
    "_id": "AndersenFamily",
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
  },
  {
    "_id": "WakefieldFamily",
    "id": "WakefieldFamily",
    "parents": [
      {
        "familyName": "Wakefield",
        "givenName": "Robin"
      },
      {
        "familyName": "Miller",
        "givenName": "Ben"
      }
    ],
    "children": [
      {
        "familyName": "Merriam",
        "givenName": "Jesse",
        "gender": "female",
        "grade": 1,
        "pets": [
          {
            "givenName": "Goofy"
          },
          {
            "givenName": "Shadow"
          }
        ]
      },
      {
        "familyName": "Miller",
        "givenName": "Lisa",
        "gender": "female",
        "grade": 8
      }
    ],
    "address": {
      "state": "NY",
      "county": "Manhattan",
      "city": "NY"
    },
    "creationDate": 1431620462,
    "isRegistered": false,
    "_partition": "Families"
  }
]

```

## sample mongosh

```mongosh
db.Families.aggregate([
  // Initial match to filter documents by lastName
  {
    $match: {
      lastName: "Andersen"  // Filter documents where lastName is "Andersen"
    }
  },
  // Project the original structure and use $filter to get the matched elements
  {
    $project: {
      original: "$$ROOT",  // Include all original fields
      // Keep the full "area.city.street.rooms" array and filter for the matched elements using $$this
      matchingRooms: {
        $filter: {
          input: "$area.city.street.rooms",
          cond: {
            $and: [
              { $eq: ["$$this.no", "001"] },  // Condition to match rooms with no = "001"
              { $lt: ["$$this.floor", 5] }    // Condition to match rooms where floor < 5
            ]
          }
        }
      },
      // Keep the full "room*no-01" array and filter for the matched elements
      matchingRoomNo01: {
        $filter: {
          input: "$room*no-01",
          cond: { $eq: ["$$this.area", 10] }  // Using $$this to match rooms with area 10
        }
      }
    }
  },
  // Match only documents where there are non-empty matched elements
  {
    $match: {
      $and: [
        { matchingRooms: { $ne: null } },  // Check that matchingRooms is not null
        { matchingRooms: { $ne: [] } },    // Check that matchingRooms is not empty
        { matchingRoomNo01: { $ne: null } },  // Check that matchingRoomNo01 is not null
        { matchingRoomNo01: { $ne: [] } }     // Check that matchingRoomNo01 is not empty
      ]
    }
  },
  // Merge the original document with the new fields using $replaceRoot and $mergeObjects
  {
    $replaceRoot: {
      newRoot: {
        $mergeObjects: [
          "$original",  // Include all original fields
          { matchingRooms: "$matchingRooms" },  // Add the filtered "matchingRooms"
          { matchingRoomNo01: "$matchingRoomNo01" }  // Add the filtered "matchingRoomNo01"
        ]
      }
    }
  },
  // Use $replaceWith to replace nested fields
  {
    $replaceWith: {
      $mergeObjects: [
        "$$ROOT",
        {
          area: {
            city: {
              street: {
                name: "$area.city.street.name",  // Preserve original street name
                rooms: "$matchingRooms"  // Replace rooms with matched rooms
              }
            }
          }
        },
        {
          "room*no-01": "$matchingRoomNo01"  // Replace room*no-01 with matched elements
        }
      ]
    }
  }
]);

```

## implementation

The above algorithm is implemented by `MongoDatabaseImpl#findWithJoin`
