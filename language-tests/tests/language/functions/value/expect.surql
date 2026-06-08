/**
[test]

[[test.results]]
value = "9"

[[test.results]]
value = "9"

[[test.results]]
error = "An error occurred: value::expect assertion failed"

[[test.results]]
error = "An error occurred: value::expect assertion failed with message: 'Should equal nine'"

[[test.results]]
error = "An error occurred: value::expect assertion failed"

[[test.results]]
value = "[{ num: 2 }, { num: 4 }, { num: 6 }]"

[[test.results]]
error = "An error occurred: value::expect assertion failed with message: 'All numbers should be positive'"
*/

// Simple assertions
9.expect(|$n| $n = 9);
9.expect(|$n| $n = 9, "Should equal nine");
10.expect(|$n| $n = 9);
10.expect(|$n| $n = 9, "Should equal nine");
10.expect(|$n| $n = 9, 1);

// .expect to assert inside of method chains
[1,2,3]
    .map(|$num| $num * 2)
    .expect(|$nums| $nums.all(|$num| $num > 0))
    .map(|$num| {
        num: $num
    });

[-1,2,3]
    .map(|$num| $num * 2)
    .expect(|$nums| $nums.all(|$num| $num > 0), "All numbers should be positive")
    .map(|$num| {
        num: $num
    });