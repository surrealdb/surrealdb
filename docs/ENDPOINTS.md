# Endpoints

This document describes RESTful http endpoints which can be used to query and manipulate the database.

---

### SQL

The SQL endpoint allows you to query the database using any SQL query supported by Surreal. The content body must contain the properly formatted SQL queries which should be executed by the database. It may include a batch of SQL statements separated by a semicolon.

- `POST` `https://api.surreal.io/sql`

---

### Key Value

The key value endpoints allow you to manipulate the database records without needing to use SQL. It only includes a small portion of the functionality available with SQL queries. The endpoints enable you to use the database as if it were an API, using multi-tenancy separation, and multi-level authentication for preventing access to specific data within a database.

- `GET` `https://api.surreal.io/key/{table}`
```sql
SELECT * FROM {table}
```

- `POST` `https://api.surreal.io/key/{table}`
```sql
CREATE {table}
```

- `DELETE` `https://api.surreal.io/key/{table}`
```sql
DELETE {table}
```

- `GET` `https://api.surreal.io/key/{table}/{key}`
```sql
SELECT * FROM @{table}:{id}
```

- `PUT` `https://api.surreal.io/key/{table}/{key}`
```sql
UPDATE @{table}:{id} CONTENT {} RETURN AFTER
```

- `POST` `https://api.surreal.io/key/{table}/{key}`
```sql
CREATE @{table}:{id} CONTENT {} RETURN AFTER
```

- `PATCH` `https://api.surreal.io/key/{table}/{key}`
```sql
MODIFY @{table}:{id} DIFF {} RETURN AFTER
```

- `TRACE` `https://api.surreal.io/key/{table}/{key}`
```sql
SELECT HISTORY FROM @{table}:{id}
```

- `DELETE` `https://api.surreal.io/key/{table}/{key}`
```sql
DELETE @{table}:{id}
```