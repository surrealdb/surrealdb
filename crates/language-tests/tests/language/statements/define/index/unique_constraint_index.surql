DEFINE INDEX name ON TABLE company COLUMNS name UNIQUE;
INSERT INTO company { name: 'SurrealDB' };
INSERT INTO company { name: 'SurrealDB' };
SELECT count() FROM company GROUP ALL;
