/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: edge:1 }, { id: edge:2 }, { id: edge:3 }, { id: edge:4 }, { id: edge:5 }]"

[[test.results]]
value = "[{ id: edge:1 }, { id: edge:2 }, { id: edge:3 }, { id: edge:4 }, { id: edge:5 }]"

[[test.results]]
value = "[{ id: edge:5 }]"

[[test.results]]
value = "[{ id: edge:1 }]"

[[test.results]]
value = "[{ id: edge:1 }, { id: edge:2 }]"

[[test.results]]
value = "[{ id: edge:2 }]"

*/

{
    CREATE a:1, a:2;
    RELATE a:1->edge:1->a:2;
    RELATE a:1->edge:2->a:2;
    RELATE a:1->edge:3->a:2;
    RELATE a:1->edge:4->a:2;
    RELATE a:1->edge:5->a:2;
	NONE
};

a:1->(SELECT id FROM edge).*;
a:1->(SELECT id FROM edge:..).*;
a:1->(SELECT id FROM edge:5..).*;
a:1->(SELECT id FROM edge:..=1).*;
a:1->(SELECT id FROM edge:1..=2).*;
a:1->(SELECT id FROM edge:..=3 START 1 LIMIT 1).*;
