/**
[test]
reason = "Test math::median aggregate with GROUP BY for even and odd counts"

[[test.results]]
value = "[{ grp: 'odd', id: item:1, val: 1 }]"

[[test.results]]
value = "[{ grp: 'odd', id: item:2, val: 3 }]"

[[test.results]]
value = "[{ grp: 'odd', id: item:3, val: 5 }]"

[[test.results]]
value = "[{ grp: 'even', id: item:4, val: 10 }]"

[[test.results]]
value = "[{ grp: 'even', id: item:5, val: 20 }]"

[[test.results]]
value = "[{ grp: 'even', id: item:6, val: 30 }]"

[[test.results]]
value = "[{ grp: 'even', id: item:7, val: 40 }]"

[[test.results]]
value = "[{ grp: 'even', median: 25f }, { grp: 'odd', median: 3 }]"

*/

CREATE item:1 SET grp = 'odd', val = 1;
CREATE item:2 SET grp = 'odd', val = 3;
CREATE item:3 SET grp = 'odd', val = 5;
CREATE item:4 SET grp = 'even', val = 10;
CREATE item:5 SET grp = 'even', val = 20;
CREATE item:6 SET grp = 'even', val = 30;
CREATE item:7 SET grp = 'even', val = 40;

SELECT grp, math::median(val) AS median FROM item GROUP BY grp ORDER BY grp;
