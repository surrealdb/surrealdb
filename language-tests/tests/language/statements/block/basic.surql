/**
[test]
reason = "Test basic block expression evaluation"

# 0: Basic block - returns last expression value
[[test.results]]
value = "3"

# 1: Block with LET - variable binding
[[test.results]]
value = "6"

# 2: Block with multiple LET statements
[[test.results]]
value = "15"

# 3: Simple nested block (no LET in inner)
[[test.results]]
value = "42"

# 4: Nested block accessing outer LET
[[test.results]]
value = "1"

# 5: Empty block returns NONE
[[test.results]]
value = "NONE"

# 6: Pure nested blocks - no LET anywhere
[[test.results]]
value = "100"

# 7: Inner block with LET (no outer LET)
[[test.results]]
value = "100"

# 8: Block with RETURN (tests fallback to legacy compute)
[[test.results]]
value = "42"

# 9: Block with LET followed by RETURN (tests fallback)
[[test.results]]
value = "11"
*/

# 0: Basic block - returns last expression value
{ 1; 2; 3 };

# 1: Block with LET - variable binding (last expression is returned)
{
	LET $x = 5;
	$x + 1
};

# 2: Block with multiple LET statements
{
	LET $a = 5;
	LET $b = 10;
	$a + $b
};

# 3: Simple nested block (no LET in inner) - just test that nested block returns value
{
	{ 42 }
};

# 4: Nested block accessing outer LET
{
	LET $x = 1;
	{ $x }
};

# 5: Empty block returns NONE
{;};

# 6: Pure nested blocks - no LET anywhere (testing nested block structure)
{
	1;
	{
		2;
		100
	}
};

# 7: Inner block with LET (no outer LET)
{
	{
		LET $y = 2;
		100
	}
};

# 8: Block with RETURN (tests fallback to legacy compute)
{
	RETURN 42
};

# 9: Block with LET followed by RETURN (tests fallback)
{
	LET $x = 10;
	RETURN $x + 1
};
