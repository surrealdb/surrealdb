/**
[test]
reason = """
Import for a_view_recalculates_rows_its_writer_cannot_select.surql: an
aggregated view over a table whose `PERMISSIONS FOR select` hides most of its
rows from a record user that may nevertheless delete any of them. Runs as
owner.
"""
run = false
*/

DEFINE TABLE reading SCHEMALESS
	PERMISSIONS
		FOR select WHERE hidden = false
		FOR create, update, delete FULL;

-- Selecting the view is granted outright, so what a reader sees is the row as
-- the maintenance stored it rather than a filtered view of it.
DEFINE TABLE peak AS
	SELECT sensor, math::max(v) AS m, count() AS n FROM reading GROUP BY sensor
	PERMISSIONS FOR select FULL;

-- Group `s` keeps a row the record user may select, so a recalculation
-- narrowed to that user finds a row and answers with the wrong maximum.
CREATE reading:s1 SET sensor = 's', v = 10, hidden = true;
CREATE reading:s2 SET sensor = 's', v = 7, hidden = true;
CREATE reading:s3 SET sensor = 's', v = 3, hidden = false;

-- Group `t` keeps none, so the same recalculation finds no row at all.
CREATE reading:t1 SET sensor = 't', v = 9, hidden = true;
CREATE reading:t2 SET sensor = 't', v = 4, hidden = true;
