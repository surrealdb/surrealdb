# Queries

This document describes example SQL queries which can be used to query the database.

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
MODIFY @person:id WITH {JSON}
```

### RELATE

```sql
RELATE friend FROM @person:one TO @person:two
RELATE friend FROM @person:one TO @person:two UNIQUE
```

### SELECT

```sql
SELECT FROM VIEW adults ON PERSON
```

```sql
SELECT * FROM person
SELECT *, both() FROM person
SELECT *, in(), out() FROM person
SELECT * FROM person WHERE account = 'abcum' ORDER BY (account, firstname, lastname)
SELECT mean(value) FROM cpu
SELECT mean(value)
SELECT mean(value) from cpu WHERE host = 'serverA' AND time > now() - 4h GROUP BY time(5m)

SELECT ALL FROM person WHERE tags ∋ "tag"
SELECT ALL FROM person WHERE tags.? = "tag"
SELECT ALL FROM person WHERE "tag" IN tags
SELECT ALL FROM person WHERE tags CONTAINS "tag"
SELECT ALL FROM person WHERE tags.? IN ["tag1", "tag2"]
SELECT ALL FROM person WHERE emails.?.value ~ "gmail.com" /* Any email address value matches 'gmail.com' */
SELECT ALL FROM person WHERE emails.*.value ~ "gmail.com" /* Every email address value matches 'gmail.com' */

SELECT ALL FROM person WHERE tags ∌ "tag"
SELECT ALL FROM person WHERE tags.* != "tag"
SELECT ALL FROM person WHERE "tag" NOT IN tags
SELECT ALL FROM person WHERE tags NOT CONTAINS "tag"
SELECT ALL FROM person WHERE tags.* NOT IN ["tag1", "tag2"]

SELECT ALL FROM person WHERE tags IN ["tag1", "tag2", "tag3"]
SELECT ALL FROM person WHERE "tag1" IN tags

SELECT *, :(friend|follow)/:person[age>=18,social=true] AS acquaintances FROM person WHERE acquaintances IN [@person:123]
SELECT *, :(friend|follow)/:person[age>=18,social=true] AS acquaintances FROM person WHERE acquaintances.firstname IN ['Tobie']

```

### FIELD

```sql
DEFINE FIELD fullname ON person CODE `
let email = doc.emails ? _(doc.emails).pluck('value').first() : null;
return [doc.firstname, doc.middlename, doc.lastname, doc.username, email].filter(i => { return i }).join(' ');
`
```

### INDEX

```sql
DEFINE INDEX name ON person COLUMNS lastname, firstname /* Define a field on */
DEFINE INDEX name ON person COLUMNS accounts, lastname, firstname, emails.0.value /* */
```

```sql
DEFINE INDEX adults ON *
MAP
`
if (meta.table == 'person') {
    if (doc.firstname && doc.lastname) {
        emit([doc.lastname, doc.firstname, meta.id], null)
    }
    if (doc:friend(person):name == 'Tobie') {
        emit()
    }
}
`
REDUCE
`
`
```

```sql
RESYNC INDEX name ON person
```

```sql
REMOVE INDEX name ON person
```