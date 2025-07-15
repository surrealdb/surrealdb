/**
[test]

[[test.results]]
value = "None"

[[test.results]]
value = "[{ id: student:1, marks: [{ mark: 40, subject: 'english' }] }]"

[[test.results]]
value = "[{ id: student:2, marks: [{ mark: 40, subject: 'maths' }, { mark: 40, subject: 'english' }, { mark: 45, subject: 'hindi' }] }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'mark_idx', operator: '=', value: 40 }, table: 'student' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: student:1, marks: [{ mark: 40, subject: 'english' }] }, { id: student:2, marks: [{ mark: 40, subject: 'maths' }, { mark: 40, subject: 'english' }, { mark: 45, subject: 'hindi' }]}]"

[[test.results]]
value = "None"

[[test.results]]
value = "[{ id: student:1, marks: [{ mark: 40, subject: 'english' }] }, { id: student:2, marks: [{ mark: 40, subject: 'maths' }, { mark: 40, subject: 'english' }, { mark: 45, subject: 'hindi' }]}]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: student:1, marks: [{ mark: 40, subject: 'english' }] }]"

*/
DEFINE INDEX mark_idx ON student COLUMNS marks.*.mark;
CREATE student:1 CONTENT {
    marks: [
        { subject: "english", mark: 40 }
    ]
};
CREATE student:2 CONTENT {
    marks: [
        { subject: "maths", mark: 40 },
        { subject: "english", mark: 40 },
        { subject: "hindi", mark: 45 }
    ]
};
SELECT * FROM student WHERE marks.*.mark CONTAINS 40 EXPLAIN;
SELECT * FROM student WITH NOINDEX WHERE marks.*.mark CONTAINS 40;
REBUILD INDEX mark_idx ON student;
SELECT * FROM student WITH NOINDEX WHERE marks.*.mark CONTAINS 40;
DELETE student:2;
SELECT * FROM student WITH NOINDEX WHERE marks.*.mark CONTAINS 40;
