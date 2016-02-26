# Storage

Surreal can be used with any key-value storage which enables range scans. This document describes how the data is stored in the storage layer, so that it can be queried and manipulated quickly and efficiently.

**Base keys**

```bash
{$base} = "surreal" # This is the base key
```

The base key is used to separate the data used in SurrealDB from data used by other databases using the same key:value store.

```bash
{$auth} = "abcum" # This is the name of the auth account
```

The auth key is used to enable multi-tenancy of databases on SurrealDB. Each authenticated user is able to access databases underneath their own auth key only.

```bash
{$db} = "database" # This is the name of the database
```

The database key is used to separate data into multiple different databases under each multi-tenant installation.

**Unique ids**

Each view, table, and index is assigned a unique id, which is used instead of the name in each key:value pair. This allows for views, indexes, and tables to be deleted asynchronously, while at the same time a new one is created in its place with the same name.

**Data types**

Each data type is stored using a different symbol in the key:value pair.

```bash
! # Used to store Surreal config data
¤ # Used to store view and index data
« # Used to store in edges
» # Used to store out edges
• # Used to store diffs to each record
‡ # Used to store time-series data
```

---

### Table

```bash
/{$base}/{$auth}/{$db}/!/tables/{$table} "{$tableid}"
# e.g.
/{$base}/{$auth}/{$db}/!/tables/people "1bd7ajq8"
```

### Field

```bash
/{$base}/{$auth}/{$db}/{$tableid}/!/field/{$field} "{CODE}"
# e.g.
/{$base}/{$auth}/{$db}/1bd7ajq8/!/field/fullname "return doc.fname + doc.lname"
```

### Items

```bash
/{$base}/{$auth}/{$db}/{$tableid}/{$id} ""
# e.g
/{$base}/{$auth}/{$db}/{$tableid}/UUID `{"name":"Tobie","age":18}`
```

*TRAIL*
```bash
/{$base}/{$auth}/{$db}/{$tableid}/•/{$id}/{$time} ""
# e.g
/{$base}/{$auth}/{$db}/{$tableid}/•/UUID/2016-01-29T22:42:56.478173947Z ""
```

*EVENT*
```bash
/{$base}/{$auth}/{$db}/{$tableid}/‡/{$id}/{$type}/{$time} ""
# e.g
/{$base}/{$auth}/{$db}/{$tableid}/‡/UUID/login/2016-01-29T22:42:56.478173947Z ""
```

*EDGES*
```bash
/{$base}/{$auth}/{$db}/{$tableid}/»/{$id}/{$type}/{$edgeid} ""
/{$base}/{$auth}/{$db}/{$typeid}/{$id} ""
/{$base}/{$auth}/{$db}/{$tableid}/«/{$id}/{$type}/{$edgeid} ""
# e.g
/{$base}/{$auth}/{$db}/{$tableid}/»/1537/follow/9563 ""
/{$base}/{$auth}/{$db}/{$typeid}/9563 `{"in":"1537","out":"5295"}`
/{$base}/{$auth}/{$db}/{$tableid}/«/5295/follow/9563 ""
```

### Views

```bash
/{$base}/{$auth}/{$db}/!/views/{$view} "{$viewid}"
# e.g.
/{$base}/{$auth}/{$db}/!/views/test "9jh1ebj4"
/{$base}/{$auth}/{$db}/!/views/test/map "emit()"
/{$base}/{$auth}/{$db}/!/views/test/red "return count()"
```

```bash
/{$base}/{$auth}/{$db}/¤/{$viewid}/[{$columns}] "{$id}"
# e.g
/{$base}/{$auth}/{$db}/¤/{$viewid}/[lastname,firstname] "@person:1342"
```

### Index

```bash
/{$base}/{$auth}/{$db}/{$tableid}/!/index/{$index} "{$indexid}"
/{$base}/{$auth}/{$db}/{$tableid}/!/index/{$index}/col "{$columns}"
# e.g
/{$base}/{$auth}/{$db}/{$tableid}/!/index/names "5gbq3hm5"
/{$base}/{$auth}/{$db}/1bd7ajq8/!/index/names/col "lastname|firstname|emails.0.value"
```

**Unique index**
```bash
/{$base}/{$auth}/{$db}/{$tableid}/¤/{$indexid}/[{$columns}]/{$id} ""
# e.g
/{$base}/{$auth}/{$db}/{$tableid}/¤/{$indexid}/[lastname,firstname]/{$id} ""
```

**Non-unique index**
```bash
/{$base}/{$auth}/{$db}/{$tableid}/¤/{$indexid}/[{$columns}] "{$id}"
# e.g
/{$base}/{$auth}/{$db}/{$tableid}/¤/{$indexid}/[lastname,firstname] "@person:1342"
```