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
      $and: [
        {"lastName": "Andersen"},  // Filter documents where lastName is "Andersen"
        { 
          "area.city.street.rooms" :{
            $elemMatch: {
              familyName: "Wakefield"
            }
          }
        }
      ]  
    }
  },
  // Sort the matched documents by a specific field, e.g., creationDate in descending order
  {
    $sort: {
      creationDate: -1  // Change this field as needed for sorting
    }
  },
  // Skip a certain number of documents
  {
    $skip: 10  // Change the number as needed to skip that many documents
  },
  // Limit the number of documents returned
  {
    $limit: 5  // Change the number as needed to limit the results
  },
  // Project the original structure and use $filter to get the matched elements
  {
    $project: {
      original: "$$ROOT",  // Include all original fields
      // Keep the full "area.city.street.rooms" array and filter for the matched elements using $$this
      "matching_area__city__street__rooms": {
        $filter: {
          input: "$area.city.street.rooms",
          cond: {
            $or: [
              { $eq: ["$$this.area", 10] },  // Using $$this to match rooms with area 10
              { $eq: ["$id", "WakefieldFamily"] }    
            ]
          }
        }
      },
      // Keep the full "room*no-01" array and filter for the matched elements
      "matching_room*no-01": {
        $filter: {
          input: "$room*no-01",
          cond: { $eq: ["$$this.area", 10] }  // Using $$this to match rooms with area 10
        }
      }
    }
  },
  // Merge the original document with the new fields using $replaceRoot and $mergeObjects
  {
    $replaceRoot: {
      newRoot: {
        $mergeObjects: [
          "$original",  // Include all original fields
          { "matching_area__city__street__rooms": "$matching_area__city__street__rooms" },  // Add the filtered "matching_area__city__street__rooms"
          { "matching_room*no-01": "$matching_room*no-01" }  // Add the filtered "matching_room*no-01"
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
                rooms: "$matching_area__city__street__rooms"  // Replace rooms with matched rooms
              }
            }
          }
        },
        {
          "room*no-01": "$matching_room*no-01"  // Replace room*no-01 with matched elements
        }
      ]
    }
  },
  // Final project stage to include only specific fields
  {
    $project: {
      id: 1,
      lastName: 1,
      parents: 1,
      children: 1,
      address: 1
    }
  }
]);

```

## implementation

The above algorithm is implemented by `MongoDatabaseImpl#findWithJoin`
