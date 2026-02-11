/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: student:1, marks: [{ mark: 40, subject: 'english' }] }]"

[[test.results]]
value = "[{ id: student:2, marks: [{ mark: 40, subject: 'maths' }, { mark: 40, subject: 'english' }, { mark: 45, subject: 'hindi' }] }]"

[[test.results]]
value = '''
'Project [ctx: Db]
└────> Scan [ctx: Db] [source: student, predicate: marks.*.mark = 40]
''''

[[test.results]]
value = '''
'Project [ctx: Db]
└────> Scan [ctx: Db] [source: student, predicate: 40 INSIDE marks.*.mark]
''''

[[test.results]]
value = "[{ id: student:1, marks: [{ mark: 40, subject: 'english' }] }, { id: student:2, marks: [{ mark: 40, subject: 'maths' }, { mark: 40, subject: 'english' }, { mark: 45, subject: 'hindi' }] }]"

[[test.results]]
value = "[{ id: student:1, marks: [{ mark: 40, subject: 'english' }] }, { id: student:2, marks: [{ mark: 40, subject: 'maths' }, { mark: 40, subject: 'english' }, { mark: 45, subject: 'hindi' }] }]"

[[test.results]]
value = "[{ id: student:1, marks: [{ mark: 40, subject: 'english' }] }, { id: student:2, marks: [{ mark: 40, subject: 'maths' }, { mark: 40, subject: 'english' }, { mark: 45, subject: 'hindi' }] }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: student:1, marks: [{ mark: 40, subject: 'english' }] }]"

[[test.results]]
value = "[{ id: student:1, marks: [{ mark: 40, subject: 'english' }] }]"

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
EXPLAIN SELECT * FROM student WHERE marks.*.mark = 40;
EXPLAIN SELECT * FROM student WHERE 40 INSIDE marks.*.mark;
SELECT * FROM student WITH NOINDEX WHERE marks.*.mark CONTAINS 40;
SELECT * FROM student WHERE marks.*.mark CONTAINS 40;
SELECT * FROM student WHERE 40 INSIDE marks.*.mark;

DELETE student:2;
SELECT * FROM student WITH NOINDEX WHERE marks.*.mark CONTAINS 40;
SELECT * FROM student WHERE marks.*.mark CONTAINS 40;
SELECT * FROM student WHERE 40 INSIDE marks.*.mark;
