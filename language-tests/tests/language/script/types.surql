CREATE article:test SET
	created_at = function() {
		return new Date('1995-12-17T03:24:00Z');
	},
	next_signin = function() {
		return new Duration('1w2d6h');
	},
	manager = function() {
		return new Record('user', 'joanna');
	},
	organisation = function() {
		return new Record('organisation', {
			alias: 'acme',
			name: 'Acme Inc',
		});
	},
	identifier = function() {
		return new Uuid('03412258-988f-47cd-82db-549902cdaffe');
	}
;
