# Queries

This document describes example SQL queries which can be used to query the database.

### FIELD

```sql
/* Example of defining a custom field */
DEFINE FIELD age ON person TYPE number MIN 0 MAX 150 NOTNULL /* Define a numeric field */
DEFINE FIELD kind ON person TYPE custom ENUM ["private","public"] DEFAULT "private" /* ... or a predefined field */
DEFINE FIELD fullname ON person TYPE string CODE "return [doc.data.firstname, doc.data.middlename, doc.data.lastname].filter(function(i) { return i; }).join(' ');" /* ... or a custom-code field */
```

```sql
/* Remove the field definition */
REMOVE FIELD fullname ON person
```

### INDEX

```sql
/* Example of defining a custom index */
DEFINE INDEX sortable ON person COLUMNS accounts, emails[0].value /* Define an index */
DEFINE INDEX sortable ON person COLUMNS accounts, emails[0].value UNIQUE /* ... or a unique index */
DEFINE INDEX sortable ON person CODE "if (doc.data.age && doc.data.age > 18) emit([doc.data.lastname, doc.data.firstname, doc.id]);" /* ... or a custom-code index */
```

```sql
/* Remove the index definition */
REMOVE INDEX sortable ON person
```

### CREATE

```sql
/* Example of creating a table */
CREATE person /* Creates a new person */
CREATE person SET age=28, name='Tobie' /* ... and sets some fields */
CREATE person CONTENT {"firstname":"Tobie", "lastname":"Morgan Hitchcock"} /* ... and sets some fields */
```


```sql
/* Example of creating a specific record */
CREATE @person:id /* Creates a the person if they do not exist */
CREATE @person:id SET age = 28, name = 'Tobie' /* ... and sets name+age */
CREATE @person:id SET age = 28, name = 'Tobie', tags = [] /* ... and sets `tags` to an empty set */
CREATE @person:id SET age = 28, name = 'Tobie', tags = ['old'] /* ... and sets `tags` to a set with 1 element */
```

```sql
/* Example of multiple records in one statement */
CREATE @person:one, @person:two /* Creates both person records if they do not exist */
```

### UPDATE

```sql
/* Example of updating a table */
UPDATE person /* Updates all person records */
UPDATE person SET age=VOID /* ... and removes the age field */
UPDATE person SET age=VOID WHERE age < 18 /* ... if the condition matches */
```

```sql
/* Example of updating a specific record */
UPDATE @person:id /* Ensures the person record exists */
UPDATE @person:id CONTENT {} /* ... and erases the record data */
UPDATE @person:id SET age = 28, name = 'Tobie' /* ... and sets name+age */
UPDATE @person:id SET age = 28, name = 'Tobie', tags = NULL /* ... and sets `tags` to NULL */
UPDATE @person:id SET age = 28, name = 'Tobie', tags = [] /* ... and sets `tags` to an empty set */
UPDATE @person:id SET age = 28, name = 'Tobie', tags = ['old'] /* ... and sets `tags` to a set with 1 element */
UPDATE @person:id SET age = 28, name = 'Tobie', tags += ['new'], tags -= ['old'] /* ... and adds 'new' to `tags` and removes 'old' from `tags */
```

```sql
/* Example of multiple records in one statement */
UPDATE @person:one, @person:two /* Ensures both person records exist */
```

### DELETE

```sql
/* Example of deleting a table */
DELETE person /* Deletes all person records */
DELETE person WHERE age < 18 /* ... if the condition matches */
```

```sql
/* Example of deleting a specific record */
DELETE @person:id /* Deletes the person record */
DELETE @person:id WHERE age < 18 /* ... if the condition matches */
```

```sql
/* Example of multiple records in one statement */
DELETE @person:one, @person:two /* Deletes both person records */
```

### MODIFY

```sql
/* Example of modifying a record with jsondiffpatch */
MODIFY @person:id DIFF {JSON}
```

### RELATE

```sql
/* Example of defining graph edges between records */
RELATE friend FROM @person:one TO @person:two /* Define a graph edge */
RELATE friend FROM @person:one TO @person:two UNIQUE /* ... or ensure only one edge of this type exists */
```

### SELECT

```sql
SELECT * FROM person
SELECT *, ->, <-, <-> FROM person

/* Examples of working with sets or arrays */

SELECT * FROM person WHERE tags ∋ "tag" /* contains "tag" */
SELECT * FROM person WHERE tags.? = "tag" /* ... any tag value is "tag" */
SELECT * FROM person WHERE "tag" IN tags
SELECT * FROM person WHERE tags CONTAINS "tag"
SELECT * FROM person WHERE tags.? IN ["tag1", "tag2"] /* ... at least one tag value is "tag1" or "tag2" */

SELECT * FROM person WHERE tags ∌ "tag" /* does not contain "tag */
SELECT * FROM person WHERE tags.* != "tag" /* ... no tag value is "tag" */
SELECT * FROM person WHERE "tag" NOT IN tags
SELECT * FROM person WHERE tags NOT CONTAINS "tag"
SELECT * FROM person WHERE tags.* NOT IN ["tag1", "tag2"] /* ... all tag values are not "tag1" and "tag2" */

/* Examples of working with objects and arrays of objects */

SELECT * FROM person WHERE emails.?.value ~ "gmail.com" /* ... any email address value matches 'gmail.com' */
SELECT * FROM person WHERE emails.*.value ~ "gmail.com" /* ... every email address value matches 'gmail.com' */

/* Examples of working with relationship paths */

SELECT *, <->(friend|follow)
SELECT *, <-likes<-person.id
SELECT *, <-friend<-person[age>=18] AS friends
SELECT * FROM person WHERE ->friend->person->click->@email:1231

SELECT *, ->friend->person[age>=18] AS acquaintances FROM person WHERE acquaintances IN [@person:test]
SELECT *, ->friend->person[age>=18] AS acquaintances FROM person WHERE acquaintances.firstname IN ['Tobie']

/* Examples of working with relationship paths and embedded objects */

SELECT * FROM person WHERE &emails.?.value->to->email->to->@email:{tobie@abcum.com} /* Anybody who has sent an email to tobie@abcum.com */
SELECT * FROM person WHERE @email:{tobie@abcum.com}->from->email.id IN emails.?.value /* Anybody who has sent an email to tobie@abcum.com */

```