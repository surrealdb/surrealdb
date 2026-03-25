/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "Test CONTAINS, CONTAINSALL, and CONTAINSANY with array fields and index (new executor)"

# 0, 1, 2: Setup test data
[[test.results]]
value = "[{ id: student:1, marks: [{ mark: 50, subject: 'maths' }, { mark: 40, subject: 'english' }, { mark: 45, subject: 'tamil' }] }]"
[[test.results]]
value = "[{ id: student:2, marks: [{ mark: 50, subject: 'maths' }, { mark: 35, subject: 'english' }, { mark: 45, subject: 'hindi' }] }]"
[[test.results]]
value = "[{ id: student:3, marks: [{ mark: 50, subject: 'maths' }, { mark: 30, subject: 'hindi' }, { mark: 45, subject: 'tamil' }] }]"


[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id] {rows: 2}
    TableScan [ctx: Db] [table: student, direction: Forward, predicate: marks.*.subject CONTAINS 'english'] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "[{ id: student:1 }, { id: student:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id] {rows: 2}
    Filter [ctx: Db] [predicate: marks.*.subject CONTAINS 'english'] {rows: 2}
        IndexScan [ctx: Db] [index: subject_idx, access: = 'english', direction: Forward] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "[{ id: student:1 }, { id: student:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id] {rows: 2}
    TableScan [ctx: Db] [table: student, direction: Forward, predicate: marks.*.subject CONTAINSALL ['hindi', 'maths']] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "[{ id: student:2 }, { id: student:3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id] {rows: 2}
    Filter [ctx: Db] [predicate: marks.*.subject CONTAINSALL ['hindi', 'maths']] {rows: 2}
        UnionIndexScan [ctx: Db] [table: student, branches: 2] {rows: 3}
            IndexScan [ctx: Db] [index: subject_idx, access: = 'hindi', direction: Forward] {rows: 2}
            IndexScan [ctx: Db] [index: subject_idx, access: = 'maths', direction: Forward] {rows: 3}

Total rows: 2"'''

[[test.results]]
value = "[{ id: student:2 }, { id: student:3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id] {rows: 2}
    TableScan [ctx: Db] [table: student, direction: Forward, predicate: marks.*.subject CONTAINSANY ['tamil', 'french']] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "[{ id: student:1 }, { id: student:3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id] {rows: 2}
    Filter [ctx: Db] [predicate: marks.*.subject CONTAINSANY ['tamil', 'french']] {rows: 2}
        UnionIndexScan [ctx: Db] [table: student, branches: 2] {rows: 2}
            IndexScan [ctx: Db] [index: subject_idx, access: = 'tamil', direction: Forward] {rows: 2}
            IndexScan [ctx: Db] [index: subject_idx, access: = 'french', direction: Forward] {rows: 0}

Total rows: 2"'''

[[test.results]]
value = "[{ id: student:1 }, { id: student:3 }]"
*/

# 0, 1, 2: Setup test data
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

# 3, 4: CONTAINS without index
EXPLAIN ANALYZE SELECT id FROM student WHERE marks.*.subject CONTAINS "english";
SELECT id FROM student WHERE marks.*.subject CONTAINS "english";
# 5, 6, 7: CONTAINS with index
DEFINE INDEX subject_idx ON student COLUMNS marks.*.subject;
EXPLAIN ANALYZE SELECT id FROM student WHERE marks.*.subject CONTAINS "english";
SELECT id FROM student WHERE marks.*.subject CONTAINS "english";

# 8, 9, 10: CONTAINSALL without index
REMOVE INDEX subject_idx ON student;
EXPLAIN ANALYZE SELECT id FROM student WHERE marks.*.subject CONTAINSALL ["hindi", "maths"];
SELECT id FROM student WHERE marks.*.subject CONTAINSALL ["hindi", "maths"];
# 11, 12, 13: CONTAINSALL with index
DEFINE INDEX subject_idx ON student COLUMNS marks.*.subject;
EXPLAIN ANALYZE SELECT id FROM student WHERE marks.*.subject CONTAINSALL ["hindi", "maths"];
SELECT id FROM student WHERE marks.*.subject CONTAINSALL ["hindi", "maths"];

# 14, 15, 16: CONTAINSANY without index
REMOVE INDEX subject_idx ON student;
EXPLAIN ANALYZE SELECT id FROM student WHERE marks.*.subject CONTAINSANY ["tamil", "french"];
SELECT id FROM student WHERE marks.*.subject CONTAINSANY ["tamil", "french"];
# 17, 18, 19: CONTAINSANY with index
DEFINE INDEX subject_idx ON student COLUMNS marks.*.subject;
EXPLAIN ANALYZE SELECT id FROM student WHERE marks.*.subject CONTAINSANY ["tamil", "french"];
SELECT id FROM student WHERE marks.*.subject CONTAINSANY ["tamil", "french"];
