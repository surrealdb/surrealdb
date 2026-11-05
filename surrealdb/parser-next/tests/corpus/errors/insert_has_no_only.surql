-- Pins that INSERT takes no ONLY. Every other record-returning statement has
-- one, so the asymmetry reads like an oversight; it is not. INSERT writes a
-- list and answers with a list, and `InsertStatement` carries no `only` field
-- for a parser clause to fill. Adding one to make the statement family uniform
-- would make this parse and quietly give INSERT a single-record cardinality
-- rule it has never had.
INSERT ONLY INTO person { name: 'tobie' };
