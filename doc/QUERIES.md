# Queries

This document describes example SQL queries which can be used to query the database.

### TABLE

```sql
/* Define the table */
DEFINE TABLE person
```

```sql
/* Remove the table */
REMOVE TABLE person
```

### FIELD

```sql
/* Example of defining a field */
DEFINE FIELD age ON person TYPE number -- Define a numeric field
DEFINE FIELD age ON person TYPE number MIN 0 MAX 100 -- ... with min and max allowed values
DEFINE FIELD age ON person TYPE number MIN 0 MAX 100 NOTNULL -- ... which can't be set to null
DEFINE FIELD age ON person TYPE number MIN 0 MAX 100 NOTNULL VALIDATE -- ... which will fail if not a number
DEFINE FIELD age ON person TYPE number MIN 0 MAX 100 NOTNULL VALIDATE READONLY -- ... which is not able to be changed once defined

DEFINE FIELD iso ON output TYPE string MATCH /[a-zA-Z0-9]+/ -- Define a field which matches a regular expresion

/* Example of defining a field with allowed values */
DEFINE FIELD kind ON address TYPE custom ENUM ["home","work"] -- Define a custom field
DEFINE FIELD kind ON address TYPE custom ENUM ["home","work"] DEFAULT "home" -- ... which defaults to 'home' if not defined

/* Example of defining a computed field */
DEFINE FIELD name ON person TYPE string CODE "return [doc.data.firstname, doc.data.lastname].join(' ');" -- Define a computed field
```

```sql
/* Remove the field definition */
REMOVE FIELD name ON person
```

### INDEX

```sql
/* Example of defining a custom index */
DEFINE INDEX sortable ON person COLUMNS name -- Define a simple index
DEFINE INDEX sortable ON person COLUMNS firstname, lastname -- Define a compound index
DEFINE INDEX sortable ON person COLUMNS firstname, lastname, emails.*.value -- Define a multi compound index
DEFINE INDEX sortable ON person COLUMNS uuid UNIQUE -- Define a unique index
```

```sql
/* Remove the index definition */
REMOVE INDEX sortable ON person
```

### ACTION

```sql
/* Example of defining a custom index */
LIVE SELECT * FROM person -- Define a simple index
LIVE SELECT * FROM person WHERE age > 18 -- ... with a conditional clause
```

```sql
/* Remove the index definition */
REMOVE INDEX sortable ON person
```

### CREATE

```sql
/* Example of creating a table */
CREATE person -- Creates a new person
CREATE person SET age=28, name='Tobie' -- ... and sets some fields
CREATE person CONTENT {"firstname":"Tobie", "lastname":"Morgan Hitchcock"} -- ... and sets some fields
```

```sql
/* Example of creating a specific record */
CREATE @person:id -- Creates a the person if they do not exist
CREATE @person:id SET age = 28, name = 'Tobie' -- ... and sets name+age
CREATE @person:id SET age = 28, name = 'Tobie', tags = [] -- ... and sets tags to an empty set
CREATE @person:id SET age = 28, name = 'Tobie', tags = ['old'] -- ... and sets tags to a set with 1 element
```

```sql
/* Example of multiple records in one statement */
CREATE @person:one, @person:two -- Creates both person records if they do not exist
```

```sql
/* Example of using embedded fields */
CREATE @person:id SET name.first = "Tobie", name.last = "Morgan Hitchcock" -- Creates a the person if they do not exist
```

### UPDATE

```sql
/* Example of updating a table */
UPDATE person -- Updates all person records
UPDATE person SET age=VOID -- ... and removes the age field
UPDATE person SET age=VOID WHERE age < 18 -- ... if the condition matches
```

```sql
/* Example of updating a specific record */
UPDATE @person:id -- Ensures the person record exists
UPDATE @person:id CONTENT {} -- ... and erases the record data
UPDATE @person:id SET age = 28, name = 'Tobie' -- ... and sets name+age
UPDATE @person:id SET age = 28, name = 'Tobie', tags = NULL -- ... and sets tags to NULL
UPDATE @person:id SET age = 28, name = 'Tobie', tags = [] -- ... and sets tags to an empty set
UPDATE @person:id SET age = 28, name = 'Tobie', tags = ['old'] -- ... and sets tags to a set with 1 element
UPDATE @person:id SET age = 28, name = 'Tobie', tags += ['new'], tags -= ['old'] -- ... and adds 'new' to tags and removes 'old' from tags
```

```sql
/* Example of multiple records in one statement */
UPDATE @person:one, @person:two -- Ensures both person records exist
```

```sql
/* Example of using embedded fields */
UPDATE @person:id SET emails = [] -- Creates a the person if they do not exist
UPDATE @person:id SET emails += {type: "work", value: "tobie@abcum.co.uk"}
UPDATE @person:id SET emails.0.value = "tobie@abcum.com"
UPDATE @person:id SET emails -= {type: "work", value: "tobie@abcum.com"}
```

### DELETE

```sql
/* Example of deleting a table */
DELETE person -- Deletes all person records
DELETE person WHERE age < 18 -- ... if the condition matches
```

```sql
/* Example of deleting a specific record */
DELETE @person:id -- Deletes the person record
DELETE @person:id WHERE age < 18 -- ... if the condition matches
```

```sql
/* Example of multiple records in one statement */
DELETE @person:one, @person:two -- Deletes both person records
```

### MODIFY

```sql
/* Example of modifying a record with jsondiffpatch */
MODIFY @person:id DIFF {JSON}
```

### RELATE

```sql
-- Example of defining graph edges between records
RELATE friend FROM @person:one TO @person:two -- Define a graph edge
RELATE friend FROM @person:one TO @person:two UNIQUE -- ... or ensure only one edge of this type exists
```

### SELECT

```sql
SELECT * FROM person -- select all people

/* Examples of working with sets or arrays */

SELECT * FROM person WHERE tags ∋ "tag" -- tags contains "tag"
SELECT * FROM person WHERE tags ~ "tag" -- tags contains "tag"
SELECT * FROM person WHERE tags CONTAINS "tag" -- tags contains "tag"
SELECT * FROM person WHERE "tag" ∈ tags -- tags contains "tag"
SELECT * FROM person WHERE "tag" IS IN tags -- tags contains "tag"

SELECT * FROM person WHERE tags ∌ "tag" -- tags does not contain "tag"
SELECT * FROM person WHERE tags !~ "tag" -- tags does not contain "tag"
SELECT * FROM person WHERE tags CONTAINS NOT "tag" -- tags does not contain "tag"
SELECT * FROM person WHERE "tag" ∉ tags -- tags does not contain "tag"
SELECT * FROM person WHERE "tag" IS NOT IN tags -- tags does not contain "tag"

SELECT * FROM person WHERE tags ⊇ ["tag1", "tag2"] -- tags contains "tag1" and "tag2"
SELECT * FROM person WHERE tags CONTAINSALL ["tag1", "tag2"] -- tags contains "tag1" and "tag2"
SELECT * FROM person WHERE tags ⊃ ["tag1", "tag2"] -- tags contains "tag1" or "tag2"
SELECT * FROM person WHERE tags CONTAINSSOME ["tag1", "tag2"] -- tags contains "tag1" or "tag2"
SELECT * FROM person WHERE tags ⊅ ["tag1", "tag2"] -- tags does not contain "tag1" or "tag2"
SELECT * FROM person WHERE tags CONTAINSNONE ["tag1", "tag2"] -- tags does not contain "tag1" or "tag2"

/* Examples of working with objects and arrays of objects */

SELECT * FROM person WHERE emails.*.value = /gmail.com$/ -- all email addresses end with 'gmail.com'
SELECT * FROM person WHERE emails.*.value != /gmail.com$/ -- no email addresses end with 'gmail.com'
SELECT * FROM person WHERE emails.*.value ?= /gmail.com$/ -- any email addresses end with 'gmail.com'

/* Examples of working with relationship paths */

SELECT ->[friend]->person FROM person

SELECT *, <->friend|follow-
SELECT *, <-likes<-person.id
SELECT *, <-friend<-person[age>=18] AS friends
SELECT * FROM person WHERE ->friend->person->click->@email:1231

SELECT * FROM person WHERE age >= @person:tobie.age - 5

SELECT *, ->friend->person[age>=18] AS acquaintances FROM person WHERE acquaintances IN [@person:test]
SELECT *, ->friend->person[age>=18] AS acquaintances FROM person WHERE acquaintances.firstname IN ['Tobie']

/* Examples of working with relationship paths and embedded objects */

SELECT * FROM person WHERE emails.*.value->to->email->to->@email:{tobie@abcum.com} -- Anybody who has sent an email to tobie@abcum.com
SELECT * FROM person WHERE @email:{tobie@abcum.com}->from->email.id IN emails.?.value -- Anybody who has sent an email to tobie@abcum.com

```
