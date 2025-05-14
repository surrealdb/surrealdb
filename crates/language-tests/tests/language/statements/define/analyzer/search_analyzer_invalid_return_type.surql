DEFINE FUNCTION fn::unsupportedReturnedType($html: string) {
	RETURN 1;
};
DEFINE ANALYZER htmlAnalyzer FUNCTION fn::unsupportedReturnedType TOKENIZERS blank,class;
RETURN search::analyze('htmlAnalyzer', '<p>This is a <em>sample</em> of HTML</p>');
