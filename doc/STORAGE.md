# Storage

This document describes how the database data is stored in the key-value storage layer;

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

#### Chars

Each data type is stored using a different symbol in the key-value pair.

```bash
! # Used to store config data
* # Used to store item data
~ # Used to store item diffs
• # Used to store item trail
« # Used to store item edges
» # Used to store item edges
¤ # Used to store index data
```

#### Keys

The keys for the data in the key-value store use the template below.

```bash
KV 		/{$kv}
NS 		/{$kv}/{$ns}
NT 		/{$kv}/{$ns}/!/t/{$tk}
NU 		/{$kv}/{$ns}/!/u/{$us}
DB 		/{$kv}/{$ns}/*/{$db}
LV 		/{$kv}/{$ns}/*/{$db}/!/l/{$lv}
SC 		/{$kv}/{$ns}/*/{$db}/!/s/{$sc}
ST 		/{$kv}/{$ns}/*/{$db}/!/s/{$sc}/!/t/{$tk}
DT 		/{$kv}/{$ns}/*/{$db}/!/t/{$tk}
DU 		/{$kv}/{$ns}/*/{$db}/!/u/{$us}
VW 		/{$kv}/{$ns}/*/{$db}/!/v/{$vw}
TB 		/{$kv}/{$ns}/*/{$db}/*/{$tb}
EV		/{$kv}/{$ns}/*/{$db}/*/{$tb}/!/e/{$ev}
FD 		/{$kv}/{$ns}/*/{$db}/*/{$tb}/!/f/{$fd}
IX 		/{$kv}/{$ns}/*/{$db}/*/{$tb}/!/i/{$ix}
Table 	/{$kv}/{$ns}/*/{$db}/*/{$tb}/*
Thing 	/{$kv}/{$ns}/*/{$db}/*/{$tb}/*/{$id}
Field 	/{$kv}/{$ns}/*/{$db}/*/{$tb}/*/{$id}/*/{$fd}
Edge	/{$kv}/{$ns}/*/{$db}/*/{$tb}/*/{$id}/»/{$tp}/{$ft}/{$fk}
Patch 	/{$kv}/{$ns}/*/{$db}/*/{$tb}/~/{$id}/{$at}
Index	/{$kv}/{$ns}/*/{$db}/*/{$tb}/¤/{$ix}/{$fd}
Point	/{$kv}/{$ns}/*/{$db}/*/{$tb}/¤/{$ix}/{$fd}/{$id}
```

The specific keys listed above are displayed with example data below.

```bash
KV 		/surreal
NS 		/surreal/abcum
NT 		/surreal/abcum/!/t/default
NU 		/surreal/abcum/!/u/tobie@abcum.com
DB 		/surreal/abcum/*/acreon
LV 		/surreal/abcum/*/acreon/!/l/name
SC 		/surreal/abcum/*/acreon/!/s/admin
ST 		/surreal/abcum/*/acreon/!/s/admin/!/t/default
DT 		/surreal/abcum/*/acreon/!/t/default
DU 		/surreal/abcum/*/acreon/!/u/tobie@abcum.com
VW 		/surreal/abcum/*/acreon/!/v/ages
TB 		/surreal/abcum/*/acreon/*/person
EV		/surreal/abcum/*/acreon/*/person/!/e/activity
FD 		/surreal/abcum/*/acreon/*/person/!/f/name.first
IX 		/surreal/abcum/*/acreon/*/person/!/i/names
Table 	/surreal/abcum/*/acreon/*/person/*
Thing 	/surreal/abcum/*/acreon/*/person/*/tobie
Field 	/surreal/abcum/*/acreon/*/person/*/tobie/*/name.first
Edge	/surreal/abcum/*/acreon/*/person/*/tobie/»/like/entity/apple
Patch 	/surreal/abcum/*/acreon/*/person/~/tobie/2016-01-29T22:42:56.478173947Z
Index	/surreal/abcum/*/acreon/*/person/¤/names/[col1,col2,col3]
Point	/surreal/abcum/*/acreon/*/person/¤/names/[col1,col2,col3]/tobie
```
