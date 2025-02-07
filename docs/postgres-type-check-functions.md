# Type Check Functions(IS_DEFINED / IS_NUMBER) implementations 



Below is a **summary table** showing how to replicate various Cosmos DB functions using **PostgreSQL**’s **JSONPath** with `jsonb_path_exists(...)`. Each row shows the Cosmos DB function, its meaning, and the PostgreSQL equivalent JSONPath expression.

Assume we have a `jsonb` column named `data` and want to check a path `x`. (For nested paths, replace `x` with `address.city.street`, etc.)



| **Cosmos DB Function** | **Meaning**                                         | **PostgreSQL (jsonb_path_exists)**                           |
| ---------------------- | --------------------------------------------------- | ------------------------------------------------------------ |
| `IS_DEFINED(x)`        | `x` is present (whether it’s null, array, etc.)     | `jsonb_path_exists(data, '$.x')` <br/>`NOT jsonb_path_exists(data, '$.x') for negative` |
| `IS_NULL(x)`           | `x` is present **and** explicitly JSON null         | `jsonb_path_exists(data, '$.x ? (@.type() == "null")')`      |
| `IS_BOOL(x)`           | `x` is present **and** a JSON boolean (true/false)  | `jsonb_path_exists(data, '$.x ? (@.type() == "boolean")')`   |
| `IS_NUMBER(x)`         | `x` is present **and** a JSON number (int/float)    | `jsonb_path_exists(data, '$.x ? (@.type() == "number")')`    |
| `IS_STRING(x)`         | `x` is present **and** a JSON string                | `jsonb_path_exists(data, '$.x ? (@.type() == "string")')`    |
| `IS_ARRAY(x)`          | `x` is present **and** a JSON array                 | `jsonb_path_exists(data, '$.x ? (@.type() == "array")')`     |
| `IS_OBJECT(x)`         | `x` is present **and** a JSON object                | `jsonb_path_exists(data, '$.x ? (@.type() == "object")')`    |
| `IS_PRIMITIVE(x)`      | `x` is present **and** a scalar (null/bool/num/str) | `jsonb_path_exists(data,  '$.x ? (    @.type() == "null" ||    @.type() == "boolean" ||    @.type() == "number" ||    @.type() == "string"  )')` |



### Notes

1. `IS_DEFINED`

   - Using `jsonb_path_exists(data, '$.x')` simply checks whether path `x` exists in the JSON. If any part of the path is missing, it returns `false`. If the path is there (even if the value is `null`), it returns `true`.

2. Type Checks

   - `@.type()` can be `"null"`, `"boolean"`, `"number"`, `"string"`, `"array"`, or `"object"`.
   - You can combine them with `||` for an OR condition (as shown for `IS_PRIMITIVE`).

3. Nested Paths

   - For nested fields (like 

     ```
     address.city.street
     ```

     ), just replace 

     ```
     $.x
     ```

      with 

     ```
     $.address.city.street
     ```

     . For example:

     ```sql
     jsonb_path_exists(data, '$.address.city.street ? (@.type() == "object")')
     ```

4. Behavior with Missing Keys

   - If `x` (or any nested field) does not exist, the path does not match, so `jsonb_path_exists` returns `false`. This aligns with Cosmos DB’s “undefined” concept for missing properties.
