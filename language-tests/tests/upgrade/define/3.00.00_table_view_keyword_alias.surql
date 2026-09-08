/**
[env]
imports = ["upgrade/define/3.00.00_table_view_keyword_alias_import.surql"]

[test]
upgrade = true
# A stored idiom is bare canonical text, and a release that reads it back as an
# expression cannot decode one whose leading segment is a reserved keyword. Only
# a hop landing on 3.4.0 or later reads it with the idiom grammar, so a hop that
# ends below it — 3.0.0 => 3.0.1, say — would be asserting a decode its target
# cannot perform. The gate bounds the upgrade target; it is not a WIP marker.
version = ">=3.4.0"
reason = """
A view's projection alias and its GROUP targets are idioms, and an idiom is
stored as its bare canonical text: `function` reaches the catalog with no
quoting to mark it an identifier. Reading that text back has to use the idiom
grammar. Read as an expression the keyword opens a statement instead, the whole
table definition fails to decode, and every path that needs the definition goes
with it — the table listing, the database listing, and the write path of the
view's source table alike.

Only a binary that writes the bare text can create that state, so the current
checkout cannot produce it and no ordinary language test reaches it; the import
axis is the only way in. The two places the definition is stored are asserted
separately, because a decode that works for one is not evidence for the other:
the copy filed under the source table, which every write to the source resolves,
and the view's own definition, which the database listing renders back as
SurrealQL with the keyword quoted again.
"""

[[test.results]]
match = '''
    string::contains($result.tables.up_view_kw_view, "DEFINE TABLE up_view_kw_view")
    && string::contains($result.tables.up_view_kw_view, "AS SELECT id, country AS `function` FROM up_view_kw_src")
'''

[[test.results]]
match = '''
    string::contains($result.tables.up_view_kw_view, "AS SELECT id, country AS `function` FROM up_view_kw_src")
    && string::contains($result.tables.up_view_kw_agg, "AS SELECT `function`, count() AS total FROM up_view_kw_gsrc")
    && string::contains($result.tables.up_view_kw_agg, "GROUP BY `function`")
'''

[[test.results]]
value = "[{ country: 'uk', id: up_view_kw_src:1 }]"

[[test.results]]
value = "[{ function: 'uk', id: up_view_kw_view:1 }]"

[[test.results]]
value = "[{ function: 'uk', id: up_view_kw_gsrc:1 }]"

[[test.results]]
value = "[{ function: 'uk', id: up_view_kw_agg:['uk'], total: 1 }]"

*/
-- The view definition is also filed under its source table, and that copy is
-- what a table listing reports. Decoding it re-reads the stored alias text.
INFO FOR TABLE up_view_kw_src;
-- The view's own definition, from the other place it is stored, re-rendered as
-- SurrealQL: the keyword comes back quoted, so the text reparses to the idiom
-- it was written from rather than to a statement.
INFO FOR DB;
-- Writing to the source is what makes the definition load-bearing rather than
-- merely readable: the write path resolves every view over the table from the
-- same stored copy before it can maintain them.
CREATE up_view_kw_src:1 SET country = 'uk';
-- The projection still names the aliased field, so the view is maintained and
-- not just decodable.
SELECT * FROM up_view_kw_view;
-- The other idiom leaf: a GROUP target that is a reserved keyword.
CREATE up_view_kw_gsrc:1 SET `function` = 'uk';
-- The aggregate groups on the stored keyword idiom and counts into it.
SELECT * FROM up_view_kw_agg;
