SELECT * FROM function() {
	throw 'error';
};
SELECT * FROM function() {
	throw new Error('error');
};
