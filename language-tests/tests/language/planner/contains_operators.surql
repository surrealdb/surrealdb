/**
[env]
new-planner-strategy = "compute-only"

[test]
reason = "Test CONTAINS, CONTAINSALL, and CONTAINSANY with array fields and index"

[[test.results]]
value = "[{ id: student:1, marks: [{ mark: 50, subject: 'maths' }, { mark: 40, subject: 'english' }, { mark: 45, subject: 'tamil' }] }]"

[[test.results]]
value = "[{ id: student:2, marks: [{ mark: 50, subject: 'maths' }, { mark: 35, subject: 'english' }, { mark: 45, subject: 'hindi' }] }]"

[[test.results]]
value = "[{ id: student:3, marks: [{ mark: 50, subject: 'maths' }, { mark: 30, subject: 'hindi' }, { mark: 45, subject: 'tamil' }] }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'student' }, operation: 'Iterate Table' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: student:1 }, { id: student:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ detail: { plan: { index: 'subject_idx', operator: '=', value: 'english' }, table: 'student' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: student:1 }, { id: student:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'student' }, operation: 'Iterate Table' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: student:2 }, { id: student:3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ detail: { plan: { index: 'subject_idx', operator: 'union', value: ['hindi', 'maths'] }, table: 'student' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: student:2 }, { id: student:3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'student' }, operation: 'Iterate Table' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: student:1 }, { id: student:3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ detail: { plan: { index: 'subject_idx', operator: 'union', value: ['tamil', 'french'] }, table: 'student' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

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
SELECT id FROM student WHERE marks.*.subject CONTAINS "english" EXPLAIN;
SELECT id FROM student WHERE marks.*.subject CONTAINS "english";
-- CONTAINS with index
DEFINE INDEX subject_idx ON student COLUMNS marks.*.subject;
SELECT id FROM student WHERE marks.*.subject CONTAINS "english" EXPLAIN;
SELECT id FROM student WHERE marks.*.subject CONTAINS "english";

-- CONTAINSALL without index
REMOVE INDEX subject_idx ON student;
SELECT id FROM student WHERE marks.*.subject CONTAINSALL ["hindi", "maths"] EXPLAIN;
SELECT id FROM student WHERE marks.*.subject CONTAINSALL ["hindi", "maths"];
-- CONTAINSALL with index
DEFINE INDEX subject_idx ON student COLUMNS marks.*.subject;
SELECT id FROM student WHERE marks.*.subject CONTAINSALL ["hindi", "maths"] EXPLAIN;
SELECT id FROM student WHERE marks.*.subject CONTAINSALL ["hindi", "maths"];

-- CONTAINSANY without index
REMOVE INDEX subject_idx ON student;
SELECT id FROM student WHERE marks.*.subject CONTAINSANY ["tamil", "french"] EXPLAIN;
SELECT id FROM student WHERE marks.*.subject CONTAINSANY ["tamil", "french"];
-- CONTAINSANY with index
DEFINE INDEX subject_idx ON student COLUMNS marks.*.subject;
SELECT id FROM student WHERE marks.*.subject CONTAINSANY ["tamil", "french"] EXPLAIN;
SELECT id FROM student WHERE marks.*.subject CONTAINSANY ["tamil", "french"];
