/**
[env]
new-planner-strategy = "compute-only"

[test]
reason = "Test that boolean expressions in WHERE with index produce correct results"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ flag: true, id: test:t }]"

[[test.results]]
value = "[{ flag: false, id: test:f }]"

[[test.results]]
value = "[{ flag: false, id: test:f }, { flag: true, id: test:t }]"

[[test.results]]
value = "[{ flag: false, id: test:f }, { flag: true, id: test:t }]"

[[test.results]]
value = "[{ flag: false, id: test:f }, { flag: true, id: test:t }]"

[[test.results]]
value = "[{ flag: false, id: test:f }, { flag: true, id: test:t }]"

[[test.results]]
value = "[{ flag: false, id: test:f }, { flag: true, id: test:t }]"
*/

DEFINE INDEX flagIndex ON TABLE test COLUMNS flag;
CREATE test:t CONTENT { flag:true };
CREATE test:f CONTENT { flag:false };
SELECT * FROM test;
SELECT * FROM test WITH NOINDEX WHERE (true OR flag=true);
SELECT * FROM test WITH NOINDEX WHERE (true OR flag==true);
SELECT * FROM test WHERE (true OR flag=true);
SELECT * FROM test WHERE (true OR flag==true);
