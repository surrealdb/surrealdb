/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ detail: { plan: { index: 'index_note_kind', operator: 'union', value: [1, 2] }, table: 'notes' }, operation: 'Iterate Index' }, { detail: { plan: { index: 'index_note_pubkey', operator: 'union', value: [123] }, table: 'notes' }, operation: 'Iterate Index' }, { detail: { plan: { direction: 'forward', from: { inclusive: false, value: 2022 }, index: 'index_note_published', to: { inclusive: false, value: NONE } }, table: 'notes' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: notes:3, kind: 1, pubkey: 123, published: 2023 }, { id: notes:5, kind: 1, pubkey: 123, published: 2025 }, { id: notes:4, kind: 2, pubkey: 123, published: 2024 }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'index_note_kind', operator: 'union', value: [1, 2] }, table: 'notes' }, operation: 'Iterate Index' }, { detail: { plan: { index: 'index_note_pubkey', operator: 'union', value: [123] }, table: 'notes' }, operation: 'Iterate Index' }, { detail: { plan: { direction: 'forward', from: { inclusive: false, value: NONE }, index: 'index_note_published', to: { inclusive: false, value: 2024 } }, table: 'notes' }, operation: 'Iterate Index' }, { detail: { plan: { direction: 'forward', from: { inclusive: false, value: 2022 }, index: 'index_note_published', to: { inclusive: false, value: NONE } }, table: 'notes' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: notes:3, kind: 1, pubkey: 123, published: 2023 }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'index_note_kind', operator: 'union', value: [1, 2] }, table: 'notes' }, operation: 'Iterate Index' }, { detail: { plan: { index: 'index_note_pubkey', operator: 'union', value: [123] }, table: 'notes' }, operation: 'Iterate Index' }, { detail: { plan: { direction: 'forward', from: { inclusive: false, value: NONE }, index: 'index_note_published', to: { inclusive: false, value: 2022 } }, table: 'notes' }, operation: 'Iterate Index' }, { detail: { plan: { direction: 'forward', from: { inclusive: false, value: 2022 }, index: 'index_note_published', to: { inclusive: false, value: NONE } }, table: 'notes' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: notes:1, kind: 1, pubkey: 123, published: 2021 }, { id: notes:3, kind: 1, pubkey: 123, published: 2023 }, { id: notes:5, kind: 1, pubkey: 123, published: 2025 }, { id: notes:4, kind: 2, pubkey: 123, published: 2024 }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'index_note_kind', operator: 'union', value: [1, 2] }, table: 'notes' }, operation: 'Iterate Index' }, { detail: { plan: { index: 'index_note_pubkey', operator: 'union', value: [123] }, table: 'notes' }, operation: 'Iterate Index' }, { detail: { plan: { direction: 'forward', from: { inclusive: false, value: NONE }, index: 'index_note_published', to: { inclusive: false, value: 2022 } }, table: 'notes' }, operation: 'Iterate Index' }, { detail: { plan: { direction: 'forward', from: { inclusive: false, value: 2022 }, index: 'index_note_published', to: { inclusive: false, value: NONE } }, table: 'notes' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: notes:1, kind: 1, pubkey: 123, published: 2021 }, { id: notes:3, kind: 1, pubkey: 123, published: 2023 }, { id: notes:5, kind: 1, pubkey: 123, published: 2025 }, { id: notes:4, kind: 2, pubkey: 123, published: 2024 }]"

*/
BEGIN;
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
COMMIT;


SELECT * FROM notes WHERE (kind IN [1,2] OR pubkey IN [123]) AND published > 2022 EXPLAIN;
SELECT * FROM notes WHERE (kind IN [1,2] OR pubkey IN [123]) AND published > 2022;
SELECT * FROM notes WHERE published < 2024 AND (kind IN [1,2] OR pubkey IN [123]) AND published > 2022 EXPLAIN;
SELECT * FROM notes WHERE published < 2024 AND (kind IN [1,2] OR pubkey IN [123]) AND published > 2022;
SELECT * FROM notes WHERE published < 2022 OR (kind IN [1,2] OR pubkey IN [123]) AND published > 2022 EXPLAIN;
SELECT * FROM notes WHERE published < 2022 OR (kind IN [1,2] OR pubkey IN [123]) AND published > 2022;
SELECT * FROM notes WHERE (kind IN [1,2] AND published < 2022) OR (pubkey IN [123] AND published > 2022) EXPLAIN;
SELECT * FROM notes WHERE (kind IN [1,2] AND published < 2022) OR (pubkey IN [123] AND published > 2022);
