/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 3}
    Filter [ctx: Db] [predicate: kind INSIDE [1, 2] OR pubkey INSIDE [123] AND published > 2022] {rows: 3}
        IndexScan [ctx: Db] [index: index_note_published, access: >2022, direction: Forward] {rows: 3}

Total rows: 3''''

[[test.results]]
value = "[{ id: notes:3, kind: 1, pubkey: 123, published: 2023 }, { id: notes:4, kind: 2, pubkey: 123, published: 2024 }, { id: notes:5, kind: 1, pubkey: 123, published: 2025 }]"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 1}
    Filter [ctx: Db] [predicate: published < 2024 AND kind INSIDE [1, 2] OR pubkey INSIDE [123] AND published > 2022] {rows: 1}
        IndexScan [ctx: Db] [index: index_note_published, access: >2022 <2024, direction: Forward] {rows: 1}

Total rows: 1''''

[[test.results]]
value = "[{ id: notes:3, kind: 1, pubkey: 123, published: 2023 }]"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 4}
    Filter [ctx: Db] [predicate: published < 2022 OR kind INSIDE [1, 2] OR pubkey INSIDE [123] AND published > 2022] {rows: 4}
        UnionIndexScan [ctx: Db] [table: notes, branches: 2] {rows: 4}
            IndexScan [ctx: Db] [index: index_note_published, access: <2022, direction: Forward] {rows: 1}
            IndexScan [ctx: Db] [index: index_note_published, access: >2022, direction: Forward] {rows: 3}

Total rows: 4''''

[[test.results]]
value = "[{ id: notes:1, kind: 1, pubkey: 123, published: 2021 }, { id: notes:3, kind: 1, pubkey: 123, published: 2023 }, { id: notes:4, kind: 2, pubkey: 123, published: 2024 }, { id: notes:5, kind: 1, pubkey: 123, published: 2025 }]"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 4}
    Filter [ctx: Db] [predicate: kind INSIDE [1, 2] AND published < 2022 OR pubkey INSIDE [123] AND published > 2022] {rows: 4}
        UnionIndexScan [ctx: Db] [table: notes, branches: 2] {rows: 5}
            IndexScan [ctx: Db] [index: index_note_published, access: <2022, direction: Forward] {rows: 1}
            IndexScan [ctx: Db] [index: index_note_pubkey, access: = 123, direction: Forward] {rows: 5}

Total rows: 4''''

[[test.results]]
value = "[{ id: notes:1, kind: 1, pubkey: 123, published: 2021 }, { id: notes:3, kind: 1, pubkey: 123, published: 2023 }, { id: notes:4, kind: 2, pubkey: 123, published: 2024 }, { id: notes:5, kind: 1, pubkey: 123, published: 2025 }]"

*/
{
	DEFINE INDEX index_note_id ON TABLE notes COLUMNS id;
	DEFINE INDEX index_note_kind ON TABLE notes COLUMNS kind;
	DEFINE INDEX index_note_pubkey ON TABLE notes COLUMNS pubkey;
	DEFINE INDEX index_note_published ON TABLE notes COLUMNS published;
	CREATE notes:1 SET kind = 1, pubkey = 123, published=2021;
	CREATE notes:2 SET kind = 2, pubkey = 123, published=2022;
	CREATE notes:3 SET kind = 1, pubkey = 123, published=2023;
	CREATE notes:4 SET kind = 2, pubkey = 123, published=2024;
	CREATE notes:5 SET kind = 1, pubkey = 123, published=2025;
	RETURN "OK";
};


EXPLAIN ANALYZE SELECT * FROM notes WHERE (kind IN [1,2] OR pubkey IN [123]) AND published > 2022;
SELECT * FROM notes WHERE (kind IN [1,2] OR pubkey IN [123]) AND published > 2022;
EXPLAIN ANALYZE SELECT * FROM notes WHERE published < 2024 AND (kind IN [1,2] OR pubkey IN [123]) AND published > 2022;
SELECT * FROM notes WHERE published < 2024 AND (kind IN [1,2] OR pubkey IN [123]) AND published > 2022;
EXPLAIN ANALYZE SELECT * FROM notes WHERE published < 2022 OR (kind IN [1,2] OR pubkey IN [123]) AND published > 2022;
SELECT * FROM notes WHERE published < 2022 OR (kind IN [1,2] OR pubkey IN [123]) AND published > 2022;
EXPLAIN ANALYZE SELECT * FROM notes WHERE (kind IN [1,2] AND published < 2022) OR (pubkey IN [123] AND published > 2022);
SELECT * FROM notes WHERE (kind IN [1,2] AND published < 2022) OR (pubkey IN [123] AND published > 2022);
