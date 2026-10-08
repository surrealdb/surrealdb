/**
[test]
reason = """
	`ONLY` asks that the statement not answer with several rows. None is not
	several: an empty source, a missing record and a predicate nothing
	satisfies all answer `NONE`, and only a second row is an error.

	An array source written without a limit is left to
	`reproductions/only_over_an_array_written_without_a_limit.surql`.
"""

# 0: a single value
[[test.results]]
value = "1"

# 1: NONE is no rows
[[test.results]]
value = "NONE"

# 2: a two element array is several rows
[[test.results]]
error = "Expected a single result output when using the ONLY keyword"

# 3: an empty array with a limit
[[test.results]]
value = "NONE"

# 4: a single element array with a limit
[[test.results]]
value = "1"

# 5: a two element array cut to one by the limit
[[test.results]]
value = "1"

# 6: two sources are several rows
[[test.results]]
error = "Expected a single result output when using the ONLY keyword"

# 7: two sources cut to one by the limit
[[test.results]]
value = "1"

*/
// 0: Test ONLY with a single value
SELECT * FROM ONLY 1;

// 1: Test ONLY with NONE
SELECT * FROM ONLY NONE;

// 2: Test ONLY with a two element array
SELECT * FROM ONLY [1, 2];

// 3: Test ONLY with an empty array and a limit
SELECT * FROM ONLY [] LIMIT 1;

// 4: Test ONLY with a single element array and a limit
SELECT * FROM ONLY [1] LIMIT 1;

// 5: Test ONLY with a two element array and a limit
SELECT * FROM ONLY [1, 2] LIMIT 1;

// 6: Test ONLY with multiple tables should error
SELECT * FROM ONLY 1, 2;

// 7: Test with multiple tables and a limit
SELECT * FROM ONLY 1, 2 LIMIT 1;
