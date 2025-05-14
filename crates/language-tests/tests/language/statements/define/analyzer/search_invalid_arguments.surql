DEFINE FUNCTION fn::unsupportedFunction() {
	RETURN 1;
};
DEFINE ANALYZER htmlAnalyzer FUNCTION fn::unsupportedFunction TOKENIZERS blank,class;
RETURN search::analyze('htmlAnalyzer', '<p>This is a <em>sample</em> of HTML</p>');
