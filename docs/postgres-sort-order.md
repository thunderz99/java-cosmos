# Sort order implementation



## the problem



postgres default string sort in jsonb does a case-insensitive sort.



* Cosmos/MongoDB:  "Galaxy", "Pixel", "iPhone"
  * this is expected as a case-sensitive sort
* PostgreSQL: "Galaxy", "iPhone", "Pixel"
  * this is not expected



## the problem in detail

using postgres JDBC, I have table("SheetContents") of "id(text)", "data(jsonb)" columns.

having  records as below in the table:

```JSON
{
  "id": "id1",
  "_ts": 1740446731.95,
  "type": "Phone",
  "_etag": "d63fb0f8-21c7-4062-98e6-dc51fc7b16e0",
  "value": "Galaxy",
  "_partition": "SheetContents"
}
{
  "id": "id2",
  "_ts": 1740446731.968,
  "type": "Phone",
  "_etag": "0a47810c-121c-425e-a1bd-097b5b018f75",
  "value": "Pixel",
  "_partition": "SheetContents"
}
{
  "id": "id3",
  "_ts": 1740446731.982,
  "type": "Phone",
  "_etag": "1ec24cfe-1cfa-4883-9fff-cbd0b59d93f6",
  "value": "iPhone",
  "_partition": "SheetContents"
}

```

SQL1:

```SQL
SELECT *
 FROM schema1."SheetContents"
 WHERE (data->>'type' = 'Phone')
 ORDER BY data->'value' COLLATE "C" ASC OFFSET 0 LIMIT 10
```

result1:

```
// expected: "Galaxy" < "Pixel" < "iPhone"
// what i get: "Galaxy" < "iPhone" < "Pixel" (which ignores upper and lower case)
```

SQL2:
```SQL
SELECT *
 FROM schema1."SheetContents"
 WHERE (data->>'type' = 'Phone')
 ORDER BY data->'value' ASC OFFSET 0 LIMIT 10
```


result2:

```
still wrong result "Galaxy" < "iPhone" < "Pixel"
```

I have to using jsonb type sort ( because the schema is dynamic, I dont know what value contains(int or string).
I am satisfied by the jsonb order ( null < booleans < numbers < strings < arrays < objects).
But the upper / lower is not as expected.


## solution1(not used)

PostgreSQL’s jsonb type uses its own internal, deterministic comparison rules that aren’t affected by collations. In other words, applying a COLLATE clause to a jsonb value doesn’t change how strings are compared internally.

When you use:

```SQL
ORDER BY data->'value' COLLATE "C" ASC
```

the COLLATE “C” is ignored because the operator is comparing jsonb values—not plain text. For jsonb values, PostgreSQL defines an order that (for strings) may effectively compare them in a case‑insensitive way (or using a normalization that doesn’t match the “C” collation’s behavior). That’s why you see the order as "Galaxy" < "iPhone" < "Pixel" instead of the expected "Galaxy" < "Pixel" < "iPhone" (which is what you’d get if you compared the text values using the “C” collation, where the ASCII codes for uppercase letters come before those for lowercase letters).

If you need to order by string values with a specific collation (and thus a specific case‑sensitive behavior), you must extract the text from the jsonb using the ->> operator. For example:

```SQL
SELECT *
  FROM schema1."SheetContents"
 WHERE data->>'type' = 'Phone'
 ORDER BY data->>'value' COLLATE "C" ASC
 OFFSET 0 LIMIT 10;
```

This works because data->>‘value’ returns a text value, and the COLLATE “C” then applies correctly, sorting "Galaxy", "Pixel", and "iPhone" as you expect.

## solution2(used)


To achieve the ordering you want—using case‑sensitive text ordering for strings while preserving the jsonb order for non‑strings—you can split the ordering into multiple keys. One common approach is to order first by a numeric mapping of the jsonb type (to preserve the overall jsonb order: null < boolean < number < string < array < object), then order strings using a COLLATE “C” expression, and finally order by the original jsonb value for non‑string types. For example:

```SQL
SELECT *
FROM unittest_postgres_pmp9."SheetContents"
WHERE data->>'type' = 'Phone'
ORDER BY
  -- Map the jsonb type to a numeric value
  CASE jsonb_typeof(data->'value')
    WHEN 'null' THEN 0
    WHEN 'boolean' THEN 1
    WHEN 'number' THEN 2
    WHEN 'string' THEN 3
    WHEN 'array' THEN 4
    WHEN 'object' THEN 5
    ELSE 6
  END,
  -- For string types, order by the text value with the desired collation
  CASE 
    WHEN jsonb_typeof(data->'value') = 'string'
      THEN data->>'value' COLLATE "C"
    ELSE NULL
  END,
  -- For non-string types, fall back on the default jsonb ordering
  data->'value'
OFFSET 0 LIMIT 10;
```

Explanation
	1.	Numeric Type Mapping:
The first ORDER BY clause maps the jsonb type (as returned by jsonb_typeof) to a numeric value. This step respects the jsonb ordering (e.g. numbers come before strings).
	2.	String-Specific Ordering:
The second ORDER BY clause uses a CASE to extract the text value (using ->>) only when the type is string, applying COLLATE "C" for the desired case-sensitive sort. For non-string types, it returns NULL so it doesn’t affect their ordering.
	3.	Fallback Ordering:
The final key orders by the original jsonb value. For non-string types, this means they are sorted according to PostgreSQL’s built‑in jsonb ordering.

This multi-key ORDER BY avoids mixing types within a single CASE expression while achieving the desired ordering behavior.


## appendix (collate settings on database)

The above sort SQL is option1. it worked.
Is there any option2, that using database/schema settings and let postgres to sort default to case-sensitive sort?

PostgreSQL does not allow you to set a default collation at the schema level. Instead, the default collation is determined by the database’s LC_COLLATE and LC_CTYPE settings. If you want all text columns in your database (and, by extension, in any schema within that database) to use the “C” collation by default, you need to create the database with those settings. For example:

```SQL
CREATE DATABASE mydb
  WITH LC_COLLATE = 'C'
       LC_CTYPE = 'C'
       TEMPLATE = template0;
```

Then, you can create your schema within that database:

```SQL
CREATE SCHEMA my_schema;
```

Now, any new table you create in this database that does not explicitly specify a collation for its text columns will use the “C” collation by default.

### Important Notes and HUGE Caveats about Option 2:

* Database-Wide Impact: Changing the database default collation is a drastic, database-wide change. It affects the default sorting and comparison behavior for all text-based operations in that entire database, not just for your SheetContents table or JSONB data.

* Breaks Existing Applications: If you have existing applications or queries that rely on the current default collation (which is often locale-aware and might be case-insensitive), changing the database default collation can break these applications or lead to unexpected and incorrect sorting behavior in other parts of your system.

* Index Incompatibility: Indexes are collation-sensitive. If you change the collation of a column or the database default collation, existing indexes on text columns might become invalid or inefficient. You might need to rebuild indexes, which can be a time-consuming and disruptive operation.

* Not Targeted Enough: Your requirement is specifically about case-sensitive sorting for certain JSONB string values within a particular query context. Changing the database default collation is a blunt instrument that affects everything, not just your specific need.

* Lack of Flexibility: If you change the database default to "C" and later need case-insensitive sorting in another part of your application, you would have to explicitly use a different COLLATE clause for those specific queries, making things potentially more complex.

* Maintenance and Portability Issues: Changing database-level settings can make your database less portable and harder to manage, especially if other parts of your system expect the standard default collation behavior.

Why Option 2 is Almost Always the WRONG Approach for Your JSONB Sorting Problem:

* Overkill: It's a massive, database-wide change to solve a very specific, query-level sorting requirement.

* Risk of Breaking Existing Functionality: High chance of negatively impacting other parts of your application or database that depend on the current default collation.

* Inflexibility: Reduces flexibility in your database and application.

* Unnecessary Complexity: Makes your database configuration more complex to manage and understand.

* Performance Implications: Potentially requires index rebuilds and might affect query performance in unexpected ways if not carefully tested.