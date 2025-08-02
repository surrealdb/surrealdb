/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: edge:[6], in: a:1, out: a:2 }, { id: edge:[7], in: a:1, out: a:2 }, { id: edge:[8], in: a:1, out: a:2 }]"

*/
LET $num = 10;

{
    CREATE a:1, a:2;
    RELATE a:1->edge:[$num - 5]->a:2;
    RELATE a:1->edge:[$num - 4]->a:2;
    RELATE a:1->edge:[$num - 3]->a:2;
    RELATE a:1->edge:[$num - 2]->a:2;
    RELATE a:1->edge:[$num - 1]->a:2;
	NONE
};

// Testing one side with a variable here to ensure computation works
SELECT * FROM a:1->edge:[6]..=[$num - 2];
