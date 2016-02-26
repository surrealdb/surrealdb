# Endpoints

This document describes RESTful http endpoints which can be used to query and manipulate the database.

---

### SQL

*Endpoints*
- `POST` `https://api.surreal.io/sql` Execute SQL contained in HTTP body

---

### Tables

*Storage*
- `/surreal/{id}/{db}/{table}`

*Retrieve*
- `SCAN` `/surreal/{id}/{db}/{table}/` `/surreal/{id}/{db}/{table}/~`

*Endpoints*
- `GET` `https://api.surreal.io/{table}` SELECT * FROM {table}
- `POST` `https://api.surreal.io/{table}` INSERT INTO {table}
- `DEL` `https://api.surreal.io/{table}` DELETE FROM {table}

---

### Records

*Storage*
- `/surreal/{id}/{db}/{table}/{key}`

*Retrieve*
- `GET` `/surreal/{id}/{db}/{table}/{key}`

*Endpoints*
- `GET` `https://api.surreal.io/{table}/{key}` SELECT * FROM @{table}:{id}
- `PUT` `https://api.surreal.io/{table}/{key}` UPDATE @{table}:{id} CONTENT {}
- `POST` `https://api.surreal.io/{table}/{key}` CREATE @{table}:{id} CONTENT {}
- `PATCH` `https://api.surreal.io/{table}/{key}` MODIFY @{table}:{id} CONTENT {}
- `TRACE` `https://api.surreal.io/{table}/{key}` SELECT HISTORY FROM @{table}:{id}
- `DEL` `https://api.surreal.io/{table}/{key}` DELETE @{table}:{id}

---

### History

*Storage*
- `/surreal/{id}/{db}/{table}/•/{key}/{time}`

*Retrieve*
-  `SCAN` `/surreal/{id}/{db}/{table}/•/{key}/` `/surreal/{id}/{db}/{table}/•/{key}/~`

*Endpoints*
- `GET` `https://api.surreal.io/{table}/{key}?time={time}` SELECT record as it looked at {time}

---

### Joins

*Storage*
- `/surreal/{id}/{db}/{table}/†/{key}/{type}/{foreignkey}`

*Retrieve*
-  `SCAN` `/surreal/{id}/{db}/{table}/†/{key}/{type}/` `/surreal/{id}/{db}/{table}/†/{key}/{type}/~`

*Endpoints*
- `PUT` `https://api.surreal.io/{table}/{key}/join/{type}` CREATE (in|out) join of {type}
- `GET` `https://api.surreal.io/{table}/{key}/join/{type}` SELECT (in|out|inout) joins of {type}
- `GET` `https://api.surreal.io/{table}/{key}/join/{type}/{joinkey}` SELECT (in|out) join of {type} with id {joinkey}
- `DEL` `https://api.surreal.io/{table}/{key}/join/{type}/{joinkey}` DELETE (in|out) join of {type} with id {joinkey}

---

### Relations

*Storage*
- `/surreal/{id}/{db}/{table}/(«|»)/{key}/{type}/{foreignkey}`

*Retrieve*
-  `SCAN` `/surreal/{id}/{db}/{table}/(«|»)/{key}/{type}/` `/surreal/{id}/{db}/{table}/(«|»)/{key}/{type}/~`

*Endpoints*
- `POST` `https://api.surreal.io/{table}/{key}/(in|out)/{type}/{vertexkey}` CREATE (in|out) relationship of {type} to {vertexkey}
- `GET` `https://api.surreal.io/{table}/{key}/(in|out|inout)/{type}` SELECT (in|out|inout) relationships of {type}
- `GET` `https://api.surreal.io/{table}/{key}/(in|out)/{type}/{edgekey}` SELECT (in|out) relationship of {type} with id {edgekey}
- `DEL` `https://api.surreal.io/{table}/{key}/(in|out)/{type}/{edgekey}` DELETE (in|out) relationship of {type} with id {edgekey}

---

### Events

*Storage*
- `/surreal/{id}/{db}/{table}/‡/{key}/{type}/{time}`

*Retrieve*
-  `SCAN` `/surreal/{id}/{db}/{table}/‡/{key}/{type}/` `/surreal/{id}/{db}/{table}/‡/{key}/{type}/~`

*Endpoints*
- `GET` `https://api.surreal.io/{table}/{key}/events/{type}` SELECT events('login') ON @{table}:{id}
- `POST` `https://api.surreal.io/{table}/{key}/events/{type}` CREATE EVENT login ON @{table}:{id} WITH CONTENT {}
- `GET` `https://api.surreal.io/{table}/{key}/events/{type}/{time}` SELECT events('login') ON @{table}:{id} WHERE time={time}
- `POST` `https://api.surreal.io/{table}/{key}/events/{type}/{time}` CREATE EVENT login ON @{table}:{id} WITH CONTENT {} AT TIME {time}
- `DEL` `https://api.surreal.io/{table}/{key}/events/{type}/{time}` DELETE events('login') ON @{table}:{id} WHERE time={time}

---

### Search
- `GET` `https://api.surreal.io/search` Select all records in table
