/**
[env]
planner-strategy = ["compute-only"]

[test]
reason = "Test CONTAINS, CONTAINSALL, and CONTAINSANY with array fields and index"

# 0, 1, 2: Setup test data
[[test.results]]
value = "[{ id: student:1, marks: [{ mark: 50, subject: 'maths' }, { mark: 40, subject: 'english' }, { mark: 45, subject: 'tamil' }] }]"
[[test.results]]
value = "[{ id: student:2, marks: [{ mark: 50, subject: 'maths' }, { mark: 35, subject: 'english' }, { mark: 45, subject: 'hindi' }] }]"
[[test.results]]
value = "[{ id: student:3, marks: [{ mark: 50, subject: 'maths' }, { mark: 30, subject: 'hindi' }, { mark: 45, subject: 'tamil' }] }]"

# 3, 4: CONTAINS without index
[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'student' }, operation: 'Iterate Table' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"
[[test.results]]
value = "[{ id: student:1 }, { id: student:2 }]"

# 5, 6, 7: CONTAINS with index
[[test.results]]
value = "NONE"
[[test.results]]
value = "[{ detail: { plan: { index: 'subject_idx', operator: '=', value: 'english' }, table: 'student' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"
[[test.results]]
value = "[{ id: student:1 }, { id: student:2 }]"

# 8, 9, 10: CONTAINSALL without index
[[test.results]]
value = "NONE"
[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'student' }, operation: 'Iterate Table' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"
[[test.results]]
value = "[{ id: student:2 }, { id: student:3 }]"

# 11, 12, 13: CONTAINSALL with index
[[test.results]]
value = "NONE"
[[test.results]]
value = "[{ detail: { plan: { index: 'subject_idx', operator: 'union', value: ['hindi', 'maths'] }, table: 'student' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"
[[test.results]]
value = "[{ id: student:2 }, { id: student:3 }]"

# 14, 15, 16: CONTAINSANY without index
[[test.results]]
value = "NONE"
[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'student' }, operation: 'Iterate Table' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"
[[test.results]]
value = "[{ id: student:1 }, { id: student:3 }]"

# 17, 18, 19: CONTAINSANY with index
[[test.results]]
value = "NONE"
[[test.results]]
value = "[{ detail: { plan: { index: 'subject_idx', operator: 'union', value: ['tamil', 'french'] }, table: 'student' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"
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

# 0, 1, 2: CONTAINS without index
SELECT id FROM student WHERE marks.*.subject CONTAINS "english" EXPLAIN;
SELECT id FROM student WHERE marks.*.subject CONTAINS "english";
# 3, 4, 5: CONTAINS with index
DEFINE INDEX subject_idx ON student COLUMNS marks.*.subject;
SELECT id FROM student WHERE marks.*.subject CONTAINS "english" EXPLAIN;
SELECT id FROM student WHERE marks.*.subject CONTAINS "english";

# 6, 7, 8: CONTAINSALL without index
REMOVE INDEX subject_idx ON student;
SELECT id FROM student WHERE marks.*.subject CONTAINSALL ["hindi", "maths"] EXPLAIN;
SELECT id FROM student WHERE marks.*.subject CONTAINSALL ["hindi", "maths"];

# 9, 10, 11: CONTAINSALL with index
DEFINE INDEX subject_idx ON student COLUMNS marks.*.subject;
SELECT id FROM student WHERE marks.*.subject CONTAINSALL ["hindi", "maths"] EXPLAIN;
SELECT id FROM student WHERE marks.*.subject CONTAINSALL ["hindi", "maths"];

# 12, 13, 14: CONTAINSANY without index
REMOVE INDEX subject_idx ON student;
SELECT id FROM student WHERE marks.*.subject CONTAINSANY ["tamil", "french"] EXPLAIN;
SELECT id FROM student WHERE marks.*.subject CONTAINSANY ["tamil", "french"];

# 15, 16, 17: CONTAINSANY with index
DEFINE INDEX subject_idx ON student COLUMNS marks.*.subject;
SELECT id FROM student WHERE marks.*.subject CONTAINSANY ["tamil", "french"] EXPLAIN;
SELECT id FROM student WHERE marks.*.subject CONTAINSANY ["tamil", "french"];
