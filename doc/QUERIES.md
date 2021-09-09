# Queries

This document describes example SQL queries which can be used to query the database.

#### USE

```sql
-- Specify a namespace to use for future sql commands
USE NAMESPACE abcum;
-- Specify a database to use for future sql commands
USE DATABASE acreon;
-- Specify a namespace and database to use in one sql query
USE NAMESPACE abcum DATABASE acreon;
```

#### INFO

```sql
-- Retrive info for the namespace
INFO FOR NAMESPACE;
-- Retrive info for the database
INFO FOR DATABASE;
-- Retrive info for a specific table
INFO FOR TABLE person;
```

#### DEFINE NAMESPACE

```sql
-- Define a namespace
DEFINE NAMESPACE abcum;
-- Remove a namespace and all data
REMOVE NAMESPACE abcum;
```

#### DEFINE DATABASE

```sql
-- Define a database
DEFINE DATABASE acreon;
-- Remove a database and all data
REMOVE DATABASE acreon;
```

#### DEFINE LOGIN

```sql
-- Define a user account on the namespace
DEFINE LOGIN `tobie@abcum.com` ON NAMESPACE PASSWORD '192837192837192837';
-- Remove a user account from the namespace
REMOVE LOGIN `tobie@abcum.com` ON NAMESPACE;

-- Define a user account on the database
DEFINE LOGIN `tobie@abcum.com` ON DATABASE PASSWORD '192837192837192837';
-- Remove a user account from the database
REMOVE LOGIN `tobie@abcum.com` ON DATABASE;
```

#### DEFINE TOKEN

```sql
-- Define a signing token on the namespace
DEFINE TOKEN `default` ON NAMESPACE TYPE HS256 VALUE "secretkey";
-- Define a signing token public key on the namespace
DEFINE TOKEN `default` ON NAMESPACE TYPE RS256 VALUE "-----BEGIN PUBLIC KEY----- MIGfMA0G...";
-- Remove a signing token from the namespace
REMOVE TOKEN `default` ON NAMESPACE;

-- Define a signing token on the database
DEFINE TOKEN `default` ON DATABASE TYPE HS256 VALUE "secretkey";
-- Define a signing token public key on the database
DEFINE TOKEN `default` ON DATABASE TYPE HS256 VALUE "-----BEGIN PUBLIC KEY----- MIGfMA0G...";
-- Remove a signing token from the database
REMOVE TOKEN `default` ON DATABASE;
```

#### DEFINE SCOPE

```sql
-- Define an authentication scope named 'account'
DEFINE SCOPE account SESSION 1h SIGNUP AS (CREATE admin SET email=$user, pass=bcrypt.generate($pass), account=(UPDATE AND UPSERT @account:$account SET name=$accountname)) SIGNIN AS (SELECT * FROM admin WHERE email=$user AND bcrypt.compare(pass, $pass));
-- Remove the authentication scope named 'account'
REMOVE SCOPE account;

-- Define an authentication scope named 'profile'
DEFINE SCOPE profile SESSION 24h SIGNUP AS (CREATE person SET email=$user, pass=bcrypt.generate($pass)) SIGNIN AS (SELECT * FROM person WHERE email=$user AND bcrypt.compare(pass, $pass));
-- Remove the authentication scope named 'profile'
REMOVE SCOPE profile;
```

#### DEFINE TABLE

```sql
-- Define a new table on the database
DEFINE TABLE person;
-- Remove a table from the database
REMOVE TABLE person;

-- Define a new table as schemaless
DEFINE TABLE items SCHEMALESS;
-- Define a new table as schemafull
DEFINE TABLE items SCHEMAFULL;

DEFINE TABLE shot SHARD ON course, hole;

SELECT * FROM shot::{course=$course, hole=$hole};
SELECT * FROM shot WHERE course = $course AND hole = $hole;

-- Define a new table as with no scope permissions
DEFINE TABLE items PERMISSIONS NONE;
-- Define a new table as with full scope permissions
DEFINE TABLE items PERMISSIONS FULL;
-- Define a new table as with advanced scope permissions
DEFINE TABLE items PERMISSIONS FOR select FULL FOR delete NONE FOR create, update WHERE $auth.type = "admin";
```

#### DEFINE FIELD

```sql
-- Define a new field on a database table
DEFINE FIELD age ON person;
-- Remove a field from a database table
REMOVE FIELD name ON person;

-- Define a new field with a type
DEFINE FIELD age ON person TYPE number;
-- Define a new embedded field with type
DEFINE FIELD name.first ON person TYPE string;
-- Define a new field on an array of objects
DEFINE FIELD emails.*.value ON person TYPE email;
-- Define a new field with min and max allowed values
DEFINE FIELD age ON person TYPE number MIN 0 MAX 100;
-- Define a new field which can not be specified as NULL
DEFINE FIELD age ON person TYPE number MIN 0 MAX 100 NOTNULL;
-- Define a new field which will fail if not the correct type
DEFINE FIELD age ON person TYPE number MIN 0 MAX 100 VALIDATE;
-- Define a new field which is not able to be changed once defined
DEFINE FIELD age ON person TYPE number MIN 0 MAX 100 NOTNULL VALIDATE READONLY;
-- Define a new field which defaults to a specified value if not defined
DEFINE FIELD country ON address TYPE string DEFAULT "GBR";
-- Define a new field into which any data put must match a regular expression
DEFINE FIELD iso ON output TYPE string MATCH /^[A-Z]{3}$/;
-- Define a new field into which any data put must match a specific set of values
DEFINE FIELD kind ON address TYPE custom ENUM ["home","work"];
-- Define a new computed field which will autoupdate when any dependent fields change
DEFINE FIELD fullname ON person TYPE string CODE "return [doc.data.firstname, doc.data.lastname].join(' ');";

-- Define a new field which can not be viewed or edited by any user authenticated by scope
DEFINE FIELD password ON person TYPE string PERMISSIONS NONE;
-- Define a new field which has specific access methods for any user authenticated by scope
DEFINE FIELD notes ON person TYPE string PERMISSIONS FOR select WHERE $auth.accountid = accountid FOR create, update, delete WHERE $auth.accountid = accountid AND $auth.type = "admin";
```

#### DEFINE INDEX

```sql
-- Define an index for a table
DEFINE INDEX sortable ON person COLUMNS name;
-- Remove an index from a table
REMOVE INDEX sortable ON person;

-- Define a unique index on a table
DEFINE INDEX sortable ON person COLUMNS uuid UNIQUE;
-- Define a compound index with multiple columns
DEFINE INDEX sortable ON person COLUMNS firstname, lastname;

-- Define an index for all values in an array set
DEFINE INDEX tags ON person COLUMNS tags.*;
-- Define an index for all values in each object in an array set
DEFINE INDEX tags ON person COLUMNS tags.*.value;
```

#### DEFINE VIEW

```sql
-- Define an aggregated view on a database
DEFINE VIEW ages AS SELECT count(*), min(age), max(age) FROM person;
-- Remove an aggregated view from a database
REMOVE VIEW ages;

-- Define an aggregated view with a where clause
DEFINE VIEW ages AS SELECT count(*), min(age), max(age) FROM person WHERE age > 18;
-- Define an aggregated view with a where clause, and a group-by clause
DEFINE VIEW ages AS SELECT count(*), min(age), max(age) FROM person WHERE age > 18 GROUP BY nationality;
-- Define an aggregated view with a where clause, and multiple group-by clauses
DEFINE VIEW ages AS SELECT count(*), min(age), max(age) FROM person WHERE age > 18 GROUP BY nationality, gender;
```

#### LIVE

```sql
-- Define a live query for a table
LIVE SELECT * FROM person;
-- Remove a live query from a table
KILL "183047103847103847";

-- Define a live query for a table, only for records which match a condition
LIVE SELECT name, age, country FROM person WHERE age > 18 AND age < 60;
```

#### CREATE

```sql
-- Create a new record
CREATE person;
-- Create a new record and set some fields
CREATE person SET age=28, name='Tobie';
-- Create a new record and merge the record content
CREATE person MERGE {"firstname":"Tobie", "lastname":"Morgan Hitchcock"};
-- Create a new record and specify the full record content
CREATE person CONTENT {"firstname":"Tobie", "lastname":"Morgan Hitchcock"};

-- Create a new specific record
CREATE @person:id;
-- Create a new specific record and set some fields
CREATE @person:id SET age = 28, name = 'Tobie';
-- Create a new specific record and set some fields, along with an empty set
CREATE @person:id SET age = 28, name = 'Tobie', tags = [];
-- Create a new specific record and set some fields, along with a set with 1 element
CREATE @person:id SET age = 28, name = 'Tobie', tags = ['old'];

-- Create multiple records in one query
CREATE person, person, person;
-- Create multiple specific records in
CREATE @person:one, @person:two;
```

#### UPDATE

```sql
-- Update a table, ensuring all defined fields are up-to-date
UPDATE person;
-- Update a table, setting a field to null on all records
UPDATE person SET age=NULL;
-- Update a table, removing a field completely from all records
UPDATE person SET age=VOID;
-- Update a table, removing a field completely from all records that match a condition
UPDATE person SET age=VOID WHERE age < 18;

-- Update a specific record, ensuring it exists
UPDATE @person:id
-- Update a specific record, and erase all record data
UPDATE @person:id CONTENT {};
-- Update a specific record, and set some fields
UPDATE @person:id SET age = 28, name = 'Tobie';
-- Update a specific record, and set a field as NULL
UPDATE @person:id SET age = 28, name = 'Tobie', tags = NULL;
-- Update a specific record, and set a field to an empty set
UPDATE @person:id SET age = 28, name = 'Tobie', tags = [];
-- Update a specific record, and set a field to a set with 1 element
UPDATE @person:id SET age = 28, name = 'Tobie', tags = ['old'];
-- Update a specific record, and add 'new' to the `tags` set and removes 'old' from the `tags` set
UPDATE @person:id SET age = 28, name = 'Tobie', tags += ['new'], tags -= ['old'];

-- Update multiple records in one query, ensuring both exist
UPDATE @person:one, @person:two;

-- Update a specific record and ensure the `emails` field is a set
UPDATE @person:id SET emails = [];
-- Update a specific record and add an object to the `emails` set
UPDATE @person:id SET emails += {type: "work", value: "tobie@abcum.co.uk"};
-- Update a specific record and set the vaue of the first object in the `emails` set
UPDATE @person:id SET emails[0].value = "tobie@abcum.com";
-- Update a specific record and remove the object from the `emails` set
UPDATE @person:id SET emails -= {type: "work", value: "tobie@abcum.com"};
```

#### DELETE

```sql
-- Delete all records in a table
DELETE person;
-- Delete all records in a table that match a condition
DELETE person WHERE age < 18;

-- Delete a specific record from a table
DELETE @person:id;
-- Delete a specific record, if the condition matches
DELETE @person:id WHERE age < 18;

-- Delete multiple records in one statement
DELETE @person:one, @person:two;
```

#### RELATE

```sql
-- Define an edge connection between two records
RELATE friend FROM @person:one TO @person:two;
-- Define an edge connection between two records, ensuring only one edge of this type exists
RELATE friend FROM @person:one TO @person:two UNIQUE;
-- Define an edge connection between two records, created in subqueries
RELATE friend FROM (CREATE person) TO (CREATE person);
```

#### BEGIN, CANCEL, COMMIT

```sql
-- Begin a new transaction
BEGIN;
-- Cancel a transaction
CANCEL;
-- Commit a transaction
COMMIT;

-- Define a unique index
DEFINE INDEX languages ON country COLUMNS languages.* UNIQUE;
CREATE @country:GBR SET name="Great Britain" languages=["english", "welsh", "scottish"];
CREATE @country:FRA SET name="France" languages=["french"];

-- Define a transaction that will fail, without any changes to the database
BEGIN;
CREATE @country:BRA SET name="Brazil" languages=["portugese"];
CREATE @country:USA SET name="United States of America" languages=["english"];
CREATE @country:DEU SET name="Germany" languages="german";
COMMIT;
```

#### LET, RETURN

```sql
-- Define a new variable as a new person record
LET person1 = (CREATE person);
-- Define a 2nd variable as a new person record
LET person2 = (CREATE person);
-- Define a 3rd variable as a graph connection between the 1st and 2nd variables
LET edge = (RELATE friend FROM $person TO $person2);
-- Return only the first two people, ignoring the graph edge
RETURN $person1, $person2;
```

#### SELECT

```sql
-- Select all records from a table
SELECT * FROM person;
-- Select all records where the condition matches
SELECT * FROM person WHERE age > 18;
-- Select all records and specify a dynamically calculated field
SELECT ((celsius*2)+30) AS fahrenheit FROM temperatues;
-- Select all records where the age is greater than the age of another specific record
SELECT * FROM person WHERE age >= person:tobie;

SELECT * FROM shot::{course=$course, hole=$hole}

-- Select all records where the `tags` set contains "tag"
SELECT * FROM person WHERE tags ∋ "tag";
SELECT * FROM person WHERE tags ~ "tag";
SELECT * FROM person WHERE tags CONTAINS "tag";
SELECT * FROM person WHERE "tag" ∈ tags;
SELECT * FROM person WHERE "tag" IS IN tags;
-- Select all records where the `tags` set does not contain "tag"
SELECT * FROM person WHERE tags ∌ "tag";
SELECT * FROM person WHERE tags !~ "tag";
SELECT * FROM person WHERE tags CONTAINS NOT "tag";
SELECT * FROM person WHERE "tag" ∉ tags;
SELECT * FROM person WHERE "tag" IS NOT IN tags;
-- Select all records where the `tags` set contains "tag1" AND "tag2"
SELECT * FROM person WHERE tags ⊇ ["tag1", "tag2"];
SELECT * FROM person WHERE tags CONTAINSALL ["tag1", "tag2"];
-- Select all records where the `tags` set contains "tag1" OR "tag2"
SELECT * FROM person WHERE tags ⊃ ["tag1", "tag2"];
SELECT * FROM person WHERE tags CONTAINSSOME ["tag1", "tag2"];
-- Select all records where the `tags` does not contain "tag1" OR "tag2"
SELECT * FROM person WHERE tags ⊅ ["tag1", "tag2"];
SELECT * FROM person WHERE tags CONTAINSNONE ["tag1", "tag2"];

-- Select all records where all email address values end with 'gmail.com'
SELECT * FROM person WHERE emails.*.value = /gmail.com$/;
-- Select all records where no email address values end with 'gmail.com'
SELECT * FROM person WHERE emails.*.value != /gmail.com$/;
-- Select all records where any email address value ends with 'gmail.com'
SELECT * FROM person WHERE emails.*.value ?= /gmail.com$/;

-- Select all person records, and all of their likes
SELECT ->likes->? FROM person;
-- Select all person records, and all of their friends
SELECT ->friend->person FROM person;
-- Select all person records, and all of the friends and followers
SELECT <->(friend, follow)->person FROM person;
-- Select all person records, and the ids of people who like each person
SELECT *, <-likes<-person.id FROM person;
-- Select all person records, and the people who like this person, who are older than 18
SELECT *, <-friend<-person[age>=18] AS friends FROM person;
-- Select only person records where a friend likes chocolate
SELECT * FROM person WHERE ->friend->person->likes->food:chocolate;
-- Select the products purchased by friends of a specific person record
SELECT ->friend->person{1..3}->purchased->product FROM @person:tobie;
-- Select all 1st, 2nd, or 3rd level people who this specific person record knows
SELECT ->knows->?{1..3} FROM @person:tobie;
-- Select all 1st, 2nd, and 3rd level people who this specific person record knows, or likes, as separet paths
SELECT ->knows->(? AS f1)->knows->(? AS f2)->(knows, likes AS e3 WHERE hot=true)->(? AS f3) FROM @person:tobie;
-- Select all person records (and their recipients), who have sent more than 5 emails
SELECT *, ->sent->email->to->person FROM person WHERE count(->sent->email->to->person) > 5;
-- Select all people who know jaime
SELECT * FROM person WHERE ->knows->@person:jaime;
-- Select all person records, and all of the adult friends
SELECT ->knows->(person WHERE age >= 18) FROM person;
-- Select other products purchased by people who purchased this laptop
SELECT <-purchased<-person->purchased->product FOLLOW DISTINCT FROM @product:laptop;
-- Select products purchased by people who have purchased the same products that we have purchased
SELECT ->purchased->product<-purchased<-person->purchased->product FOLLOW DISTINCT FROM @person:tobie;
-- Select products purchased by people in the last 3 weeks who have purchased the same products that we have purchased
SELECT ->purchased->product<-purchased<-person->(purchased WHERE created_at > now() - 3w)->product FOLLOW DISTINCT FROM @person:tobie;
-- Select products purchased by people who have purchased the same products that we have purchased
SELECT ->purchased->product<-purchased<-person->purchased->product FOLLOW DISTINCT FROM @person:tobie;
-- Select all people who have sent an email to tobie@abcum.com
SELECT * FROM person WHERE @email:{tobie@abcum.com}->from->email.address IN emails.?.value;
```
