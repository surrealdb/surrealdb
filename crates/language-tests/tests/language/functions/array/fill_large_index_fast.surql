/**
[env]
timeout = 100

[test]
reason = """
The below function operated in O(n) where N was the last index of the insert, eventhough the actual array was very small.
If the array is tiny this function should not take more time then it needs to set all the required values to the given value.
"""

[[test.results]]
value = "[1, 2, 10, 10, 10]"
*/
array::fill([1,2,3,4,5], 10, 2, 9_223_372_036_854_775_807);
