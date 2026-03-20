/**

[test]
wip = true

[test.results]
parsing-error = true

*/

// This should be a syntax error, but it currently accepted.
{
	1
	1;
}
