/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ fail: false, id: test:1 }]"

[[test.results]]
error = "Error while processing event test: An error occurred: Failed"

*/

DEFINE EVENT test ON test WHEN true THEN {
    IF fail {
        THROW "Failed";
    }
};

CREATE test:1 SET fail = false;
CREATE test:2 SET fail = true;