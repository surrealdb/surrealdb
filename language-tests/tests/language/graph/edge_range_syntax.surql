/**
[test]
reason = "Test edge ID range syntax for selecting edges by ID range"

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

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: computed_edge:[6], in: b:1, out: b:2 }, { id: computed_edge:[7], in: b:1, out: b:2 }]"
*/

-- Setup test data
{
    CREATE a:1, a:2;
    RELATE a:1->edge:1->a:2;
    RELATE a:1->edge:2->a:2;
    RELATE a:1->edge:3->a:2;
    RELATE a:1->edge:4->a:2;
    RELATE a:1->edge:5->a:2;
    NONE
};

-- Select all edges
a:1->(SELECT id FROM edge).*;

-- Select edges with range (all)
a:1->(SELECT id FROM edge:..).*;

-- Select edges starting from 5
a:1->(SELECT id FROM edge:5..).*;

-- Select edges up to 1 (inclusive)
a:1->(SELECT id FROM edge:..=1).*;

-- Select edges in range 1..=2
a:1->(SELECT id FROM edge:1..=2).*;

-- Select edges with range, START and LIMIT
a:1->(SELECT id FROM edge:..=3 START 1 LIMIT 1).*;

-- Computed edge ranges
LET $num = 10;

{
    CREATE b:1, b:2;
    RELATE b:1->computed_edge:[$num - 5]->b:2;
    RELATE b:1->computed_edge:[$num - 4]->b:2;
    RELATE b:1->computed_edge:[$num - 3]->b:2;
    NONE
};

-- Testing one side with a variable
SELECT * FROM b:1->computed_edge:[6]..=[$num - 2];

