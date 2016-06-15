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
/{$kv}/!/n/{$ns} ""
# e.g.
/{$kv}/!/n/acme ""
```

**Database**
```bash
/{$kv}/!/d/{$ns}/{$db} ""
# e.g.
/{$kv}/!/d/{$ns}/test ""
```

**Table**
```bash
/{$kv}/!/t/{$ns}/{$db}/{$tb} ""
# e.g.
/{$kv}/!/t/{$ns}/{$db}/people ""
```

**Field** 

```bash
/{$kv}/!/f/{$ns}/{$db}/{$tb}/{$fld} "{}"
# e.g.
/{$kv}/!/f/{$ns}/{$db}/{$tb}/fullname `{
	"name": "fullname",
	"type": "string",
	"code": "",
	"min": "",
	"max": "",
	"default": "",
	"notnull": false,
	"readonly": false,
	"mandatory": false,
}`
```

**Field**

```bash
/{$kv}/!/i/{$ns}/{$db}/{$tb}/{$idx} "{}"
# e.g.
/{$kv}/!/i/{$ns}/{$db}/{$tb}/fullname `{
	"name": "fullname",
	"code": "",
	"cols": ["firstname", "middlename", "lastname"],
	"uniq": false,
}`
```

---

### Items

```bash
/{$kv}/{$ns}/{$db}/{$tb}/{$id} "{}"
# e.g
/{$kv}/{$ns}/{$db}/{$tb}/UUID `{"name":"Tobie","age":18}`
```

*TRAIL*
```bash
/{$kv}/{$ns}/{$db}/{$tb}/•/{$id}/{$time} "{}"
# e.g
/{$kv}/{$ns}/{$db}/{$tb}/•/UUID/2016-01-29T22:42:56.478173947Z `{}`
```

*EVENT*
```bash
/{$kv}/{$ns}/{$db}/{$tb}/‡/{$id}/{$type}/{$time} "{}"
# e.g
/{$kv}/{$ns}/{$db}/{$tb}/‡/UUID/login/2016-01-29T22:42:56.478173947Z `{}`
```

*EDGES*
```bash
/{$kv}/{$ns}/{$db}/{$tableid}/»/{$id}/{$type}/{$edgeid} ""
/{$kv}/{$ns}/{$db}/{$typeid}/{$id} "{}"
/{$kv}/{$ns}/{$db}/{$tableid}/«/{$id}/{$type}/{$edgeid} ""
# e.g
/{$kv}/{$ns}/{$db}/{$tableid}/»/1537/follow/9563 ""
/{$kv}/{$ns}/{$db}/{$typeid}/9563 `{"in":"1537","out":"5295"}`
/{$kv}/{$ns}/{$db}/{$tableid}/«/5295/follow/9563 ""
```

---

### Index

**Unique index**
```bash
/{$kv}/{$ns}/{$db}/{$table}/¤/{$index}/[{$columns}] "{$id}"
# e.g
/{$kv}/{$ns}/{$db}/{$table}/¤/{$index}/[lastname,firstname] `@person:1342`
```

### Point

**Non-unique index**
```bash
/{$kv}/{$ns}/{$db}/{$table}/¤/{$index}/[{$columns}]/{$id} "{$id}"
# e.g
/{$kv}/{$ns}/{$db}/{$table}/¤/{$index}/[lastname,firstname]/{$id} `@person:1342`
```