/**
[test]
reason = """
Setup for table_view_keyword_alias.surql: the old binary stores two views that
name a reserved keyword in a position an idiom occupies — once as a projection
alias, once as a GROUP target. Both are held in the catalog as bare canonical
idiom text, with nothing in the bytes marking `function` as an identifier
rather than the start of a statement.

Each view gets its own source table, and both sources are left empty with
nothing following the two definitions. The binary that writes these definitions
cannot read them back, and the definition is copied under the source table as
well, so a row written here — or any further statement touching either source —
would fail during the import instead of leaving the state the test reads.
"""
run = false
*/
-- A projection alias that is a reserved keyword. The alias is the idiom leaf of
-- the view's field list, and reaches storage as the bare text `function`.
DEFINE TABLE up_view_kw_src SCHEMALESS;
DEFINE TABLE up_view_kw_view AS
    SELECT id, country AS `function` FROM up_view_kw_src;

-- The same keyword as a GROUP target, which is the other idiom leaf a view
-- carries. It sits on its own source table so neither definition can affect the
-- other's source.
DEFINE TABLE up_view_kw_gsrc SCHEMALESS;
DEFINE TABLE up_view_kw_agg AS
    SELECT `function`, count() AS total FROM up_view_kw_gsrc GROUP BY `function`;
