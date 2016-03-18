# Storage

Surreal can be used with any key-value storage which enables range scans. This document describes how the data is stored in the storage layer, so that it can be queried and manipulated quickly and efficiently.

**Base keys**

```bash
{$kv} = "surreal" # This is the base key
```

The base key is used to separate the data used in SurrealDB from data used by other databases using the same key:value store.

```bash
{$ns} = "acme" # This is the name of the namespace
```

The namespace key is used to enable separation of data and multi-tenancy of databases on SurrealDB.

```bash
{$db} = "test" # This is the name of the database
```

The database key is used to separate data into multiple different databases under each multi-tenant installation.

**Unique ids**

Each view, table, and index is assigned a unique id, which is used instead of the name in each key:value pair. This allows for views, indexes, and tables to be deleted asynchronously, while at the same time a new one is created in its place with the same name.

**Data types**

Each data type is stored using a different symbol in the key:value pair.

```bash
! # Used to store Surreal config data
* # Used to store item data
~ # Used to store item diffs
¤ # 
« # Used to store item edges
» # Used to store item edges
• # Used to store item events
‹ # Used to store item links
› # Used to store item links
∆ # Used to store index data
```

---

### Config

**Namespace**
```bash
/{$kv}/!/n/{$ns} "{$ns:id}"
# e.g.
/{$kv}/!/n/acme "6qh3iwp5"
```

**Database**
```bash
/{$kv}/!/d/{$ns}/{$db} "{$db:id}"
# e.g.
/{$kv}/!/d/{$ns}/test "3gt4yqk3"
```

**Table**
```bash
/{$kv}/!/t/{$ns}/{$db}/{$tb} "{$tb:id}"
# e.g.
/{$kv}/!/t/{$ns}/{$db}/people "1bd7ajq8"
```

**Field**
```bash
/{$kv}/!/f/{$ns}/{$db}/{$tb}/{$fld} "{$code}"
# e.g.
/{$kv}/!/f/{$ns}/{$db}/{$tb}/fullname "return doc.fname + doc.lname"
```

**Index**

```bash
/{$kv}/!/i/{$ns}/{$db}/{$tb}/{$idx} "{$idx:id}"
/{$kv}/!/i/{$ns}/{$db}/{$tb}/{$idx}/map "{$code:map}"
/{$kv}/!/i/{$ns}/{$db}/{$tb}/{$idx}/red "{$code:red}"
# e.g.
/{$kv}/!/i/{$ns}/{$db}/{$tb}/test "9jh1ebj4"
/{$kv}/!/i/{$ns}/{$db}/{$tb}/test/map "emit()"
/{$kv}/!/i/{$ns}/{$db}/{$tb}/test/red "return count()"
```

```bash
/{$kv}/!/i/{$ns}/{$db}/{$tb}/{$idx} "{$idx:id}"
/{$kv}/!/i/{$ns}/{$db}/{$tb}/{$idx}/col:{$cd} "{$column}"
# e.g
/{$kv}/!/i/{$ns}/{$db}/{$tb}/names "5gbq3hm5"
/{$kv}/!/i/{$ns}/{$db}/{$tb}/names/col1 "lastname"
/{$kv}/!/i/{$ns}/{$db}/{$tb}/names/col2 "firstname"
/{$kv}/!/i/{$ns}/{$db}/{$tb}/names/col3 "emails.0.value"
```

---

### Items

```bash
/{$kv}/{$ns}/{$db}/{$tb}/{$id} ""
# e.g
/{$kv}/{$ns}/{$db}/{$tb}/UUID `{"name":"Tobie","age":18}`
```

*TRAIL*
```bash
/{$kv}/{$ns}/{$db}/{$tb}/•/{$id}/{$time} ""
# e.g
/{$kv}/{$ns}/{$db}/{$tb}/•/UUID/2016-01-29T22:42:56.478173947Z ""
```

*EVENT*
```bash
/{$kv}/{$ns}/{$db}/{$tb}/‡/{$id}/{$type}/{$time} ""
# e.g
/{$kv}/{$ns}/{$db}/{$tb}/‡/UUID/login/2016-01-29T22:42:56.478173947Z ""
```

*EDGES*
```bash
/{$kv}/{$ns}/{$db}/{$tableid}/»/{$id}/{$type}/{$edgeid} ""
/{$kv}/{$ns}/{$db}/{$typeid}/{$id} ""
/{$kv}/{$ns}/{$db}/{$tableid}/«/{$id}/{$type}/{$edgeid} ""
# e.g
/{$kv}/{$ns}/{$db}/{$tableid}/»/1537/follow/9563 ""
/{$kv}/{$ns}/{$db}/{$typeid}/9563 `{"in":"1537","out":"5295"}`
/{$kv}/{$ns}/{$db}/{$tableid}/«/5295/follow/9563 ""
```

### Index

**Global index**
```bash
/{$kv}/{$ns}/{$db}/¤/{$index}/[{$columns}] "{$id}"
# e.g
/{$kv}/{$ns}/{$db}/¤/{$index}/[lastname,firstname] "@person:1342"
```

**Unique index**
```bash
/{$kv}/{$ns}/{$db}/{$table}/¤/{$index}/[{$columns}]/{$id} ""
# e.g
/{$kv}/{$ns}/{$db}/{$table}/¤/{$index}/[lastname,firstname]/{$id} ""
```

**Non-unique index**
```bash
/{$kv}/{$ns}/{$db}/{$table}/¤/{$index}/[{$columns}] "{$id}"
# e.g
/{$kv}/{$ns}/{$db}/{$table}/¤/{$index}/[lastname,firstname] "@person:1342"
```