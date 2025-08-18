/**
[env]
imports = ["harness/assert.surql"]
# This tast can take a bit longer so give it a longer timeout.
timeout = 8000

[test]
reason = "Check that ORDER BY RAND() returns  uniform samples"

[[test.results]]
value = "'OK'"

*/

BEGIN;

let $COUNT = 1000;
let $RANGE = 10;
let $SAMPLE = 5;

FOR $i in 0..$RANGE {
    CREATE type::thing("result",$i) CONTENT { count: 0, };
};

FOR $i in 0..$COUNT{
    FOR $v in (SELECT * FROM <array> 0..$RANGE ORDER BY RAND() LIMIT $SAMPLE) {
        UPDATE type::thing("result",$v) SET count += 1;
    }
};

// All the possible options should get roughly the same amount of samples.
// So we test if they are all roughly around 500 samples.
FOR $i in 0..$RANGE{
    let $cur = type::thing("result",$i);
    fn::assert($cur.count > 400, "The count should be larger then 400, got " + <string> $cur.count);
    fn::assert($cur.count < 600, "The count should be smaller then 600, got " + <string> $cur.count);
};

RETURN "OK";
COMMIT;

