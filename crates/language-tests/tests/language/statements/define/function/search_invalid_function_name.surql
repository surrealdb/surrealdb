/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
error = "The function 'fn::doesNotExist' does not exist"

*/
        DEFINE ANALYZER htmlAnalyzer FUNCTION fn::doesNotExist TOKENIZERS blank,class;
		RETURN search::analyze('htmlAnalyzer', '<p>This is a <em>sample</em> of HTML</p>');
