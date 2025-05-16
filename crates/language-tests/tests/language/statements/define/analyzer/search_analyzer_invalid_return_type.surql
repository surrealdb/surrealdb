/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "There was a problem running the unsupportedReturnedType() function. The function should return a string."

*/
DEFINE FUNCTION fn::unsupportedReturnedType($html: string) {
	RETURN 1;
};
DEFINE ANALYZER htmlAnalyzer FUNCTION fn::unsupportedReturnedType TOKENIZERS blank,class;
RETURN search::analyze('htmlAnalyzer', '<p>This is a <em>sample</em> of HTML</p>');
