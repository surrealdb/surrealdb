/**
[test]

[[test.results]]
value = "[{ id: r:[5u8], kind: 'u8' }]"

[[test.results]]
value = "[{ id: r:[5i16], kind: 'i16' }]"

[[test.results]]
value = "[{ id: r:[5], kind: 'int' }]"

[[test.results]]
value = "[{ id: r:[5u64], kind: 'u64' }]"

[[test.results]]
value = "[{ id: r:[1.5f16], kind: 'f16' }]"

[[test.results]]
value = "[{ id: r:[1.5f32], kind: 'f32' }]"

[[test.results]]
value = "[{ id: r:[1.5f], kind: 'float' }]"

[[test.results]]
value = "[{ comp_type: 'float', id: r:[1.5f], kind: 'float' }, { comp_type: 'f16', id: r:[1.5f16], kind: 'f16' }, { comp_type: 'f32', id: r:[1.5f32], kind: 'f32' }, { comp_type: 'int', id: r:[5], kind: 'int' }, { comp_type: 'u8', id: r:[5u8], kind: 'u8' }, { comp_type: 'i16', id: r:[5i16], kind: 'i16' }, { comp_type: 'u64', id: r:[5u64], kind: 'u64' }]"

[[test.results]]
value = "[{ id: r:[5u8], kind: 'u8' }]"

[[test.results]]
value = "[{ id: r:[5], kind: 'int' }]"

[[test.results]]
value = "[{ id: r:[1.5f16], kind: 'f16' }]"

[[test.results]]
value = "[{ id: ro:{ a: 10u8, b: 1.5f16 }, note: 'sized' }]"

[[test.results]]
value = "[{ id: ro:{ a: 10, b: 1.5f }, note: 'wide' }]"

[[test.results]]
value = "[{ id: ro:{ a: 10, b: 1.5f }, note: 'wide' }, { id: ro:{ a: 10u8, b: 1.5f16 }, note: 'sized' }]"
*/

-- Array and object record IDs preserve the exact number kind of each component
CREATE r:[5u8] SET kind = 'u8';
CREATE r:[5i16] SET kind = 'i16';
CREATE r:[5] SET kind = 'int';
CREATE r:[5u64] SET kind = 'u64';
CREATE r:[1.5f16] SET kind = 'f16';
CREATE r:[1.5f32] SET kind = 'f32';
CREATE r:[1.5f] SET kind = 'float';

SELECT id, kind, type::of((<array> record::id(id))[0]) AS comp_type FROM r ORDER BY id;

-- Direct point lookup must match exact component kind
SELECT id, kind FROM r:[5u8];
SELECT id, kind FROM r:[5];
SELECT id, kind FROM r:[1.5f16];

-- Object record IDs preserve number kinds
CREATE ro:{ a: 10u8, b: 1.5f16 } SET note = 'sized';
CREATE ro:{ a: 10, b: 1.5f } SET note = 'wide';
SELECT id, note FROM ro ORDER BY id;
