/**
[test]

[[test.results]]
value = "[1, 2, 3, 3]"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "{1, 2, 3}"

[[test.results]]
value = "{1, 2, 3}"

[[test.results]]
value = "[3]"

[[test.results]]
value = "[3]"

[[test.results]]
value = "{3,}"

[[test.results]]
value = "{3,}"

[[test.results]]
error = "Cannot perform addition with '[1]' and '1'"

[[test.results]]
error = "Cannot perform addition with '{1,}' and '1'"

[[test.results]]
error = "Cannot perform subtraction with '[1]' and '1'"

[[test.results]]
error = "Cannot perform subtraction with '{1,}' and '1'"

# inc_array
[[test.results]]
value = "10"

# inc_set
[[test.results]]
value = "{10,}"

# add_to_array
[[test.results]]
value = "[10]"

[[test.results]]
value = "[10, 20]"

# add_to_set
[[test.results]]
value = "{10,}"

[[test.results]]
value = "{10, 20}"

# dec_array1
[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "[2, 3]"

# dec_array2
[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "[1, 2, 3, 1]"

# dec_set
[[test.results]]
value = "{1, 2, 3}"

[[test.results]]
value = "{2, 3}"

[[test.results]]
value = "{3,}"

*/

[1, 2] + [3, 3];
[1, 2] + {3, 3};
{1, 2} + [3, 3];
{1, 2} + {3, 3};

[1, 2, 3] - [1, 2];
[1, 2, 3] - {1, 2};
{1, 2, 3} - [1, 2];
{1, 2, 3} - {1, 2};

[1] + 1;
{1,} + 1;
[1] - 1;
{1,} - 1;

CREATE ONLY t:inc_array SET v += 10 RETURN VALUE v;

CREATE ONLY t:inc_set SET v += {10,} RETURN VALUE v;

CREATE ONLY t:add_to_array SET v = [10] RETURN VALUE v;
UPDATE ONLY t:add_to_array SET v += 20 RETURN VALUE v;

CREATE ONLY t:add_to_set SET v = {10,} RETURN VALUE v;
UPDATE ONLY t:add_to_set SET v += 20 RETURN VALUE v;

CREATE ONLY t:dec_array1 SET v = [1, 2, 3] RETURN VALUE v;
UPDATE ONLY t:dec_array1 SET v -= {1,} RETURN VALUE v;

CREATE ONLY t:dec_array2 SET v = [1, 2, 3] RETURN VALUE v;
UPDATE ONLY t:dec_array2 SET v += {1,} RETURN VALUE v;

CREATE ONLY t:dec_set SET v = {1, 2, 3} RETURN VALUE v;
UPDATE ONLY t:dec_set SET v -= {1,} RETURN VALUE v;
UPDATE ONLY t:dec_set SET v -= [2] RETURN VALUE v;