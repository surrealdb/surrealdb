/**
[env]
remote-skip = "returns `duration::max` (u64::MAX seconds); the wire protocol counts duration seconds in an i64, so the value has no representation over the API and every SDK client is refused it rather than handed a wrapped one"

[test]

[[test.results]]
value = "584942417355y3w5d7h15s999ms999µs999ns"

[[test.results]]
error = "Failed to compute: \"584942417355y3w5d7h15s999ms999µs999ns + 1ns\", as the operation results in an arithmetic overflow."

*/
duration::max;
/// 1 nano second higher than maximum duration
duration::max + 1ns;