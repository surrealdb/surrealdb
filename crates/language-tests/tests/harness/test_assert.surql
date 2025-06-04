/**
[env]
imports = ["harness/assert.surql"]

[test]

[[test.results]]
value = "NONE"

[[test.results]]
error = "Execution halted with output: `should throw`"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Execution halted with output: `1 != 2 should throw`"
*/

fn::assert(true, "should not throw");
fn::assert(false, "should throw");
fn::assert_eq(1,1, "should not throw");
fn::assert_eq(1,2, "should throw");
