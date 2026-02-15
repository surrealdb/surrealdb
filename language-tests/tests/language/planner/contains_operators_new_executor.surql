/**
[test]
reason = "Test CONTAINS, CONTAINSALL, and CONTAINSANY with array fields and index (new executor)"

[[test.results]]
value = "[{ id: student:1, marks: [{ mark: 50, subject: 'maths' }, { mark: 40, subject: 'english' }, { mark: 45, subject: 'tamil' }] }]"

[[test.results]]
value = "[{ id: student:2, marks: [{ mark: 50, subject: 'maths' }, { mark: 35, subject: 'english' }, { mark: 45, subject: 'hindi' }] }]"

[[test.results]]
value = "[{ id: student:3, marks: [{ mark: 50, subject: 'maths' }, { mark: 30, subject: 'hindi' }, { mark: 45, subject: 'tamil' }] }]"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 2}
    TableScan [ctx: Db] [table: student, direction: Forward, predicate: marks.*.subject CONTAINS 'english'] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "[{ id: student:1 }, { id: student:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 2}
    TableScan [ctx: Db] [table: student, direction: Forward, predicate: marks.subject CONTAINS 'english'] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "[{ id: student:1 }, { id: student:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 2}
    TableScan [ctx: Db] [table: student, direction: Forward, predicate: marks.*.subject CONTAINSALL ['hindi', 'maths']] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "[{ id: student:2 }, { id: student:3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 2}
    TableScan [ctx: Db] [table: student, direction: Forward, predicate: marks.subject CONTAINSALL ['hindi', 'maths']] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "[{ id: student:2 }, { id: student:3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 2}
    TableScan [ctx: Db] [table: student, direction: Forward, predicate: marks.*.subject CONTAINSANY ['tamil', 'french']] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "[{ id: student:1 }, { id: student:3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 2}
    TableScan [ctx: Db] [table: student, direction: Forward, predicate: marks.subject CONTAINSANY ['tamil', 'french']] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "[{ id: student:1 }, { id: student:3 }]"
*/

CREATE student:1 CONTENT {
	marks: [
		{ subject: "maths", mark: 50 },
		{ subject: "english", mark: 40 },
		{ subject: "tamil", mark: 45 }
	]
};
CREATE student:2 CONTENT {
	marks: [
		{ subject: "maths", mark: 50 },
		{ subject: "english", mark: 35 },
		{ subject: "hindi", mark: 45 }
	]
};
CREATE student:3 CONTENT {
	marks: [
		{ subject: "maths", mark: 50 },
		{ subject: "hindi", mark: 30 },
		{ subject: "tamil", mark: 45 }
	]
};

-- CONTAINS without index
EXPLAIN ANALYZE SELECT id FROM student WHERE marks.*.subject CONTAINS "english";
SELECT id FROM student WHERE marks.*.subject CONTAINS "english";
-- CONTAINS with index
DEFINE INDEX subject_idx ON student COLUMNS marks.*.subject;
EXPLAIN ANALYZE SELECT id FROM student WHERE marks.
subject CONTAINS "english";

SELECT id FROM student WHERE marks.*.subject CONTAINS "english";

-- CONTAINSALL without index
REMOVE INDEX subject_idx ON student;
EXPLAIN ANALYZE SELECT id FROM student WHERE marks.*.subject CONTAINSALL ["hindi", "maths"];
SELECT id FROM student WHERE marks.*.subject CONTAINSALL ["hindi", "maths"];
-- CONTAINSALL with index
DEFINE INDEX subject_idx ON student COLUMNS marks.*.subject;
EXPLAIN ANALYZE SELECT id FROM student WHERE marks.
subject CONTAINSALL ["hindi", "maths"];

SELECT id FROM student WHERE marks.*.subject CONTAINSALL ["hindi", "maths"];

-- CONTAINSANY without index
REMOVE INDEX subject_idx ON student;
EXPLAIN ANALYZE SELECT id FROM student WHERE marks.*.subject CONTAINSANY ["tamil", "french"];
SELECT id FROM student WHERE marks.*.subject CONTAINSANY ["tamil", "french"];
-- CONTAINSANY with index
DEFINE INDEX subject_idx ON student COLUMNS marks.*.subject;
EXPLAIN ANALYZE SELECT id FROM student WHERE marks.
subject CONTAINSANY ["tamil", "french"];

SELECT id FROM student WHERE marks.*.subject CONTAINSANY ["tamil", "french"];
