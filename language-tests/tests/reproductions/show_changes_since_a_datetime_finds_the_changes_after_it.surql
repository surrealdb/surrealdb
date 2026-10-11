/**
[test]
reason = """
	`SHOW CHANGES ... SINCE <datetime>` reads the change sets committed at or
	after that instant, on every storage engine.

	A change set is keyed by the stamp the storage engine's timestamp oracle
	minted for its commit, and the datetime is converted into that same stamp
	space to find where the scan starts. Converted into any other space, the
	start lands somewhere else entirely: a count of nanoseconds, read against
	stamps that hold milliseconds shifted left by sixteen bits, is some fifteen
	times the stamp minted at the same instant, so a datetime from this century
	starts the scan centuries after every change and the read comes back empty.

	The table and its record are both written after 2020 and before 2100, so
	the read since 2020 returns exactly what the read since versionstamp 0
	does, and the read since 2100 returns nothing. 1970 is the zero of every
	stamp space, so the read since it cannot tell a wrong conversion from a
	right one; it stands as the control.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: t:1 }]"

[[test.results]]
match = '''
	array::len($result) == 2
	&& $result[0].changes[0].define_table.name == 't'
	&& $result[1].changes == [{ update: { a: 1, id: t:1 } }]
'''

[[test.results]]
match = '''
	array::len($result) == 2
	&& $result[0].changes[0].define_table.name == 't'
	&& $result[1].changes == [{ update: { a: 1, id: t:1 } }]
'''

[[test.results]]
match = '''
	array::len($result) == 2
	&& $result[0].changes[0].define_table.name == 't'
	&& $result[1].changes == [{ update: { a: 1, id: t:1 } }]
'''

[[test.results]]
value = "[]"

*/

DEFINE TABLE t CHANGEFEED 1h;
CREATE t:1 SET a = 1;
SHOW CHANGES FOR TABLE t SINCE 0;
SHOW CHANGES FOR TABLE t SINCE d'2020-01-01T00:00:00Z';
SHOW CHANGES FOR TABLE t SINCE d'1970-01-01T00:00:00Z';
SHOW CHANGES FOR TABLE t SINCE d'2100-01-01T00:00:00Z';
