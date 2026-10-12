//! Frozen packed rows and the descriptors they are read against.
//!
//! A packed row carries no format version of its own: its descriptor's generation is the only
//! thing a reader reads it through, and a descriptor is never rewritten. So a release that wrote a
//! row, or a descriptor, the next release reads differently would misread every row of that
//! generation silently. These bytes pin both from both sides. Each descriptor's bytes decode to the
//! descriptor it was captured from and are what it encodes to now, and each row's bytes decode,
//! whole and field by field, to the record it was packed from and are what [`pack`] writes for it
//! now.
//!
//! The cases cover every physical kind and presence, a `NULL` held by presence bits and one held
//! in a `Value` slice, both datetime escapes and the duration escape, open nodes at the root and
//! nested with their remainders, absent and `NULL` object nodes, 2-byte end offsets, an edge, and a
//! generation that takes two LEB128 bytes.

use std::sync::Arc;

use surrealdb_kvs::value::KVValue;
use surrealdb_types::Decimal;

use super::{
	PackedRow, Pres, Spec, at, bytes, check_fields, datetime, decimal, descriptor, duration, float,
	hex, int, leaf, named, node, object, packed_of, point, record, same, string, uuid,
	wide_literals,
};
use crate::Generation;
use crate::layout::{LayoutDescriptor, PhysicalKind, RowLayout};
use crate::packed::{Packing, pack};
use crate::record::{RowFlags, StoredRecord};
use crate::val::{Array, Object, RecordIdKey, TableName, Value};

/// One frozen row: its descriptor, the generation and flags it is stamped with, and the record it
/// was packed from, whose root `id` the row leaves out.
struct Case {
	name: &'static str,
	layout: LayoutDescriptor,
	generation: u16,
	flags: RowFlags,
	data: Object,
}

fn case(
	name: &'static str,
	open: bool,
	children: Vec<(&str, Spec)>,
	generation: u16,
	flags: RowFlags,
	data: Object,
) -> Case {
	Case {
		name,
		layout: LayoutDescriptor {
			table: TableName::new("fixture"),
			layout: RowLayout::Packed(descriptor(open, &named(children))),
		},
		generation,
		flags,
		data,
	}
}

fn literal(kind: PhysicalKind, presence: Pres, literals: Vec<Value>) -> Spec {
	Spec::Leaf {
		kind,
		presence,
		literals,
		tables: Vec::new(),
	}
}

fn keyed(presence: Pres, tables: &[&str]) -> Spec {
	Spec::Leaf {
		kind: PhysicalKind::RecordKey,
		presence,
		literals: Vec::new(),
		tables: tables.iter().map(|table| TableName::new(*table)).collect(),
	}
}

fn cases() -> Vec<Case> {
	use PhysicalKind as K;
	use Pres::{Both, Nullable, Optional, Required};
	vec![
		case(
			"fixed_kinds",
			false,
			vec![
				("a", leaf(K::Bool, Required)),
				("b", leaf(K::I8, Required)),
				("c", leaf(K::I16, Optional)),
				("d", leaf(K::I32, Nullable)),
				("e", leaf(K::I64, Both)),
				("f", leaf(K::U8, Required)),
				("g", leaf(K::U16, Required)),
				("h", leaf(K::U32, Required)),
				("i", leaf(K::U64, Required)),
				("j", leaf(K::F32, Required)),
				("k", leaf(K::F64, Required)),
				("l", leaf(K::Decimal16, Required)),
				("m", leaf(K::Fixed(3), Required)),
				("n", leaf(K::Uuid, Required)),
				("o", leaf(K::Point, Required)),
				(
					"p",
					literal(
						K::LiteralCode(1),
						Required,
						vec![string("red"), string("green"), int(1), float(1.0)],
					),
				),
				("q", leaf(K::DatetimeWide, Required)),
				("r", leaf(K::Bool, Optional)),
				("s", leaf(K::Datetime, Required)),
				("t", leaf(K::Duration, Required)),
				("u", leaf(K::Bool, Both)),
			],
			1,
			RowFlags::NONE,
			object(vec![
				("a", Value::Bool(true)),
				("b", int(-128)),
				("c", int(12_345)),
				("d", Value::Null),
				("e", int(-1)),
				("f", int(255)),
				("g", int(65_535)),
				("h", int(4_294_967_295)),
				("i", int(i64::MAX)),
				("j", float(1.5)),
				("k", float(-0.0)),
				("l", decimal(Decimal::new(1_234_500, 4))),
				("m", bytes(&[1, 2, 3])),
				("n", uuid(0x0123_4567_89ab_cdef_fedc_ba98_7654_3210)),
				("o", point(1.0, -2.5)),
				("p", float(1.0)),
				("q", datetime(at(1_709_210_096, 789_000_000))),
				("s", datetime(at(1_609_459_200, 0))),
				("t", duration(std::time::Duration::from_secs(5_400))),
				("u", Value::Bool(false)),
			]),
		),
		case(
			"variable_kinds",
			false,
			vec![
				("a", leaf(K::String, Required)),
				("b", leaf(K::Bytes, Optional)),
				("c", keyed(Required, &["person", "user"])),
				("d", keyed(Optional, &["person"])),
				("e", leaf(K::DecimalVar, Required)),
				("f", leaf(K::Value, Required)),
				("g", leaf(K::Value, Both)),
				("h", leaf(K::String, Nullable)),
				("i", literal(K::LiteralCode(2), Required, wide_literals())),
			],
			2,
			RowFlags::NONE,
			object(vec![
				("a", string("hello, 名前")),
				("b", bytes(&[0, 255])),
				("c", record("user", RecordIdKey::Array(Array(vec![string("a"), int(1)])))),
				("d", record("person", RecordIdKey::Number(42))),
				("e", decimal(Decimal::new(-1, 3))),
				("f", Value::Null),
				(
					"g",
					Value::Object(object(vec![
						("x", Value::Array(Array(vec![int(1), string("two")]))),
						("y", Value::Bool(true)),
					])),
				),
				("h", Value::Null),
				("i", string("l299")),
			]),
		),
		case(
			"escapes",
			false,
			vec![
				("a", leaf(K::Datetime, Required)),
				("b", leaf(K::Datetime, Optional)),
				("c", leaf(K::Datetime, Required)),
				("d", leaf(K::Duration, Required)),
				("e", leaf(K::Duration, Optional)),
				("f", leaf(K::Datetime, Required)),
			],
			3,
			RowFlags::NONE,
			object(vec![
				("a", datetime(at(-400_000_000_000, 1))),
				("b", datetime(chrono::DateTime::from_timestamp_nanos(i64::MIN))),
				("c", datetime(at(400_000_000_000, 999_999_999))),
				("d", duration(std::time::Duration::MAX)),
				("e", duration(std::time::Duration::from_nanos(u64::MAX))),
				("f", datetime(at(-1, 0))),
			]),
		),
		case(
			"nested_open",
			true,
			vec![
				("a", leaf(K::I64, Optional)),
				(
					"n",
					node(
						Both,
						true,
						vec![
							("x", leaf(K::String, Required)),
							("y", node(Optional, false, vec![("z", leaf(K::Bool, Required))])),
						],
					),
				),
			],
			4,
			RowFlags::NONE,
			object(vec![
				("a", int(1)),
				("extra", string("root")),
				("id", record("fixture", RecordIdKey::Number(1))),
				("list", Value::Array(Array(vec![int(1), int(2)]))),
				(
					"n",
					Value::Object(object(vec![
						("more", float(2.5)),
						("x", string("in")),
						("y", Value::Object(object(vec![("z", Value::Bool(true))]))),
					])),
				),
			]),
		),
		case(
			"absent_and_null",
			false,
			vec![
				("a", leaf(K::I64, Optional)),
				(
					"n",
					node(
						Nullable,
						false,
						vec![("s", leaf(K::String, Required)), ("x", leaf(K::I64, Required))],
					),
				),
				("o", node(Optional, true, vec![("p", leaf(K::Datetime, Required))])),
				("z", leaf(K::Value, Optional)),
			],
			5,
			RowFlags::NONE,
			object(vec![("n", Value::Null)]),
		),
		case(
			"two_byte_ends",
			false,
			vec![("a", leaf(K::String, Required)), ("b", leaf(K::Bytes, Required))],
			6,
			RowFlags::NONE,
			object(vec![("a", string(&"x".repeat(300))), ("b", bytes(&[7, 7, 7]))]),
		),
		case(
			"edge_at_generation_300",
			false,
			vec![("in", leaf(K::I64, Required)), ("out", leaf(K::String, Optional))],
			300,
			RowFlags::EDGE,
			object(vec![("in", int(7)), ("out", string("o"))]),
		),
	]
}

/// FROZEN. Descriptor bytes and row bytes, in hex, by the name of the case in [`cases`] they were
/// captured from. Never edit these by hand and never regenerate them to make a failing test pass:
/// a failure here means the current code no longer reads, or no longer writes, what an earlier
/// build of the packed format wrote.
#[rustfmt::skip]
const FROZEN: &[(&str, &str, &str)] = &[
	(
		"fixed_kinds",
		"fe00808010270200000009000207666978747572650427010000000227050000\
		00630365036703690302151500000025004a0073009c00c500ea000f01340159\
		017e01a301c801ed011202370274029902c202e7020c03260500060c12180214\
		010007610426010001000626010001000826010001800a260100010026050006\
		0c12180214010007620426010002000626010001000826010002820a26010001\
		00260500060c161c021401000763042601000300062601000226010002800826\
		010002830a2601000100260500060c161c021401000764042601000400062601\
		000326010002810826010002850a2601000100260500060c161c021401000765\
		042601000500062601000426010002820826010002890a260100010026050006\
		0c12180214010007660426010006000626010001000826010002910a26010001\
		00260500060c1218021401000767042601000700062601000100082601000292\
		0a2601000100260500060c121802140100076804260100080006260100010008\
		26010002940a2601000100260500060c12180214010007690426010009000626\
		010001000826010002980a2601000100260500060c121802140100076a042601\
		000c000626010001000826010002a00a2601000100260500060c121802140100\
		076b042601000d000626010001000826010002a40a2601000100260500060c12\
		1802140100076c042601000e000626010001000826010002ac0a260100010026\
		0500060c121802140100076d042601000f830626010001000826010002bc0a26\
		01000100260500060c121802140100076e042601001000062601000100082601\
		0002bf0a2601000100260500060c121802140100076f04260100150006260100\
		01000826010002cf0a2601000100260600060c12181e02140100077004260100\
		16810626010001000826010002df0a26010001000c140400040a0b0772656407\
		677265656e81050000803f260500060c12180214010007710426010012000626\
		010001000826010002e00a2601000100260500060c161c021401000772042601\
		000100062601000226010002840826010001810a2601000100260500060c1218\
		0214010007730426010011000626010001000826010002ec0a26010001002605\
		00060c12180214010007740426010013000626010001000826010002f40a2601\
		000100260500060c161c02140100077504260100010006260100042601000285\
		0826010001820a26010001000487068308fc0a80",
		"ff0101250180393000000000ffffffffffffffffffffffffffffffffffffffff\
		ffff7f0000c03f00000000000000800000040044d61200000000000000000001\
		02030123456789abcdeffedcba9876543210000000000000f03f000000000000\
		04c003f079e06500000000402f072f00007c789df2551600f01449e9040000",
	),
	(
		"variable_kinds",
		"fe00808010270200000009000207666978747572650427010000000227050000\
		00630965096709690902150900000025004e008500ba00df0004012d01560126\
		0500060c12180214010007610426010017000626010001000826010003800a26\
		01000100260500060c161c021401000762042601001800062601000226010002\
		800826010003810a2601000100260600060c12181e0214010007630426010019\
		000626010001000826010003820a26010001000e1402000707706572736f6e07\
		75736572260600060c161c220214010007640426010019000626010002260100\
		02810826010003830a26010001000e14010007706572736f6e260500060c1218\
		021401000765042601001a000626010001000826010003840a26010001002605\
		00060c1218021401000766042601001b000626010001000826010003850a2601\
		000100260500060c161c021401000767042601001b0006260100042601000282\
		0826010003860a2601000100260500060c161c02140100076804260100170006\
		2601000326010002840826010003870a2601000100270600000006000c001200\
		18001e000214010007690426010016820626010001000826010002810a260100\
		01000c152c0100000300060009000c000f001200150018001b001e0022002600\
		2a002e00320036003a003e00420046004a004e00520056005a005e0062006600\
		6a006e00720076007a007e00820086008a008e00920096009a009e00a200a600\
		aa00ae00b200b600ba00be00c200c600ca00ce00d200d600da00de00e200e600\
		ea00ee00f200f600fa00fe00020106010a010e01120116011a011e0122012601\
		2a012e01320136013a013e01420146014a014e01520156015a015e0162016601\
		6a016e01720176017a017e01820186018b01900195019a019f01a401a901ae01\
		b301b801bd01c201c701cc01d101d601db01e001e501ea01ef01f401f901fe01\
		030208020d02120217021c02210226022b02300235023a023f02440249024e02\
		530258025d02620267026c02710276027b02800285028a028f02940299029e02\
		a302a802ad02b202b702bc02c102c602cb02d002d502da02df02e402e902ee02\
		f302f802fd02020307030c03110316031b03200325032a032f03340339033e03\
		430348034d03520357035c03610366036b03700375037a037f03840389038e03\
		930398039d03a203a703ac03b103b603bb03c003c503ca03cf03d403d903de03\
		e303e803ed03f203f703fc03010406040b04100415041a041f04240429042e04\
		330438043d04420447044c04510456045b04600465046a046f04740479047e04\
		830488048d04920497049c04a104a604ab04b004b504ba04bf04c404c904ce04\
		d304d804dd04e204e704ec04f104f604fb04000505050a050f05140519051e05\
		230528052d05320537053c05410546054b05500555055a055f0564056905076c\
		30076c31076c32076c33076c34076c35076c36076c37076c38076c39076c3130\
		076c3131076c3132076c3133076c3134076c3135076c3136076c3137076c3138\
		076c3139076c3230076c3231076c3232076c3233076c3234076c3235076c3236\
		076c3237076c3238076c3239076c3330076c3331076c3332076c3333076c3334\
		076c3335076c3336076c3337076c3338076c3339076c3430076c3431076c3432\
		076c3433076c3434076c3435076c3436076c3437076c3438076c3439076c3530\
		076c3531076c3532076c3533076c3534076c3535076c3536076c3537076c3538\
		076c3539076c3630076c3631076c3632076c3633076c3634076c3635076c3636\
		076c3637076c3638076c3639076c3730076c3731076c3732076c3733076c3734\
		076c3735076c3736076c3737076c3738076c3739076c3830076c3831076c3832\
		076c3833076c3834076c3835076c3836076c3837076c3838076c3839076c3930\
		076c3931076c3932076c3933076c3934076c3935076c3936076c3937076c3938\
		076c3939076c313030076c313031076c313032076c313033076c313034076c31\
		3035076c313036076c313037076c313038076c313039076c313130076c313131\
		076c313132076c313133076c313134076c313135076c313136076c313137076c\
		313138076c313139076c313230076c313231076c313232076c313233076c3132\
		34076c313235076c313236076c313237076c313238076c313239076c31333007\
		6c313331076c313332076c313333076c313334076c313335076c313336076c31\
		3337076c313338076c313339076c313430076c313431076c313432076c313433\
		076c313434076c313435076c313436076c313437076c313438076c313439076c\
		313530076c313531076c313532076c313533076c313534076c313535076c3135\
		36076c313537076c313538076c313539076c313630076c313631076c31363207\
		6c313633076c313634076c313635076c313636076c313637076c313638076c31\
		3639076c313730076c313731076c313732076c313733076c313734076c313735\
		076c313736076c313737076c313738076c313739076c313830076c313831076c\
		313832076c313833076c313834076c313835076c313836076c313837076c3138\
		38076c313839076c313930076c313931076c313932076c313933076c31393407\
		6c313935076c313936076c313937076c313938076c313939076c323030076c32\
		3031076c323032076c323033076c323034076c323035076c323036076c323037\
		076c323038076c323039076c323130076c323131076c323132076c323133076c\
		323134076c323135076c323136076c323137076c323138076c323139076c3232\
		30076c323231076c323232076c323233076c323234076c323235076c32323607\
		6c323237076c323238076c323239076c323330076c323331076c323332076c32\
		3333076c323334076c323335076c323336076c323337076c323338076c323339\
		076c323430076c323431076c323432076c323433076c323434076c323435076c\
		323436076c323437076c323438076c323439076c323530076c323531076c3235\
		32076c323533076c323534076c323535076c323536076c323537076c32353807\
		6c323539076c323630076c323631076c323632076c323633076c323634076c32\
		3635076c323636076c323637076c323638076c323639076c323730076c323731\
		076c323732076c323733076c323734076c323735076c323736076c323737076c\
		323738076c323739076c323830076c323831076c323832076c323833076c3238\
		34076c323835076c323836076c323837076c323838076c323839076c32393007\
		6c323931076c323932076c323933076c323934076c323935076c323936076c32\
		3937076c323938076c3239390485068008830a88",
		"ff0102072b010d0f17181a1b2d2d68656c6c6f2c20e5908de5898d00ff011402\
		0002076181aa8301011a02000b017814020001810774776f017903",
	),
	(
		"escapes",
		"fe00808010270200000009000207666978747572650426010002260500eff1f3\
		f502140600254e7398c1260500060c1218021401000761042601001100062601\
		0001000826010002810a2601000100260500060c161c02140100076204260100\
		1100062601000226010002800826010002890a2601000100260500060c121802\
		14010007630426010011000626010001000826010002910a2601000100260500\
		060c12180214010007640426010013000626010001000826010002990a260100\
		0100260500060c161c0214010007650426010013000626010002260100028108\
		26010002a10a2601000100260500060c12180214010007660426010011000626\
		010001000826010002a90a26010001000482068008b10a80",
		"ff41030300000000000000800000000000000080ffffffffffffff7fffffffff\
		ffffffffffffffffffffffff003665c4ffffffff00006024dea2ffffff010000\
		0001fb823edafdffffff00f2a7080200a0db215d000000ffc99a3b03ffffffff\
		ffffffffffc99a3b0409fa824b04000000ffe54a2a",
	),
	(
		"nested_open",
		"fe00808010270200000009000207666978747572650427010000000227050000\
		00320134013601380102150700000026004f007800a100c900f5002605000309\
		1319021400042601001b00062601000226010002800826010003800a26010002\
		00260500060c161c021401000761042601000500062601000226010002810826\
		010002820a2601000100260500060c161c02140100076e042601002200062601\
		000426010002820826010004000a2601000100260500060c161c02140100076e\
		042601001b00062601000226010002840826010003810a260100020026050009\
		0f151b0214020002076e07780426010017000626010001000826010003820a26\
		01000100260500090f191f0214020002076e0779042601002200062601000226\
		010002850826010004000a26010001002605000c12181e021403000204076e07\
		79077a0426010001000626010001000826010001800a26010001000486068108\
		8a0a83",
		"ff0104370101000000000000001724261a02000b05657874726107726f6f7404\
		6c6973741d01021a0100046d6f72650500002040696e",
	),
	(
		"absent_and_null",
		"fe00808010270200000009000207666978747572650427010000000227050000\
		0059015b015d015f01021508000000290052007a00a200cb00f4001c01260500\
		060c161c02140100076104260100050006260100022601000280082601000281\
		0a2601000100260500060c161c02140100076e04260100220006260100032601\
		0002810826010004000a2601000100260500090f151b0214020002076e077304\
		26010017000626010001000826010003800a2601000100260500090f151b0214\
		020002076e07780426010005000626010001000826010002890a260100010026\
		0500060c161c02140100076f0426010022000626010002260100028208260100\
		04000a2601000100260500060c161c02140100076f042601001b000626010002\
		26010002830826010003810a2601000200260500090f151b0214020002076f07\
		700426010011000626010001000826010002910a2601000100260500060c161c\
		02140100077a042601001b00062601000226010002840826010003820a260100\
		01000485068008990a83",
		"ff010500000000000000000000000000000000000000000000000000000000",
	),
	(
		"two_byte_ends",
		"fe008080102602000902076669787475726504260100022605004f5153550214\
		020025260500060c121802140100076104260100170006260100010008260100\
		03800a2601000100260500060c12180214010007620426010018000626010001\
		000826010003810a26010001000480068008800a82",
		"ff02062c012f0178787878787878787878787878787878787878787878787878\
		7878787878787878787878787878787878787878787878787878787878787878\
		7878787878787878787878787878787878787878787878787878787878787878\
		7878787878787878787878787878787878787878787878787878787878787878\
		7878787878787878787878787878787878787878787878787878787878787878\
		7878787878787878787878787878787878787878787878787878787878787878\
		7878787878787878787878787878787878787878787878787878787878787878\
		7878787878787878787878787878787878787878787878787878787878787878\
		7878787878787878787878787878787878787878787878787878787878787878\
		78787878787878787878787878787878787878070707",
	),
	(
		"edge_at_generation_300",
		"fe0080801026020009020766697874757265042601000226050056585a5c0214\
		020026260500070d13190214010007696e042601000500062601000100082601\
		0002810a2601000100260500080e181e02140100076f75740426010017000626\
		01000226010002800826010003800a26010001000481068008890a81",
		"ff09ac02010700000000000000016f",
	),
];

fn unhex(hex: &str) -> Vec<u8> {
	assert_eq!(hex.len() % 2, 0, "an odd number of hex digits");
	(0..hex.len())
		.step_by(2)
		.map(|at| u8::from_str_radix(&hex[at..at + 2], 16).expect("hex"))
		.collect()
}

fn frozen(name: &str) -> (Vec<u8>, Vec<u8>) {
	let (_, layout, row) = FROZEN
		.iter()
		.find(|(n, _, _)| *n == name)
		.unwrap_or_else(|| panic!("no frozen bytes for {name}"));
	(unhex(layout), unhex(row))
}

fn generation(case: &Case) -> Generation {
	Generation::new(case.generation).expect("not zero")
}

/// [`cases`] and [`FROZEN`] name the same rows in the same order, so a case is never added without
/// its bytes nor its bytes dropped.
#[test]
fn every_case_has_frozen_bytes() {
	let names: Vec<&str> = cases().iter().map(|case| case.name).collect();
	let frozen: Vec<&str> = FROZEN.iter().map(|(name, _, _)| *name).collect();
	assert_eq!(names, frozen);
}

/// Each frozen descriptor decodes, through the read every descriptor takes, to the descriptor it
/// was captured from.
#[test]
fn frozen_descriptors_decode() {
	for case in cases() {
		let (bytes, _) = frozen(case.name);
		let decoded = LayoutDescriptor::kv_decode_value(&bytes, ()).unwrap_or_else(|error| {
			panic!(
				"BACKWARDS COMPATIBILITY BROKEN: the frozen descriptor {} no longer decodes: {error}",
				case.name
			)
		});
		assert_eq!(decoded, case.layout, "{} decodes to another descriptor", case.name);
	}
}

/// Each frozen row decodes whole to the record it was packed from, with and without its id, and
/// reads field by field to what the whole decode holds.
#[test]
fn frozen_rows_decode() {
	for case in cases() {
		let (_, bytes) = frozen(case.name);
		let packed = packed_of(&case.layout);
		let record = StoredRecord::parse(&bytes)
			.expect("parses")
			.unwrap_or_else(|| panic!("{} is not in the v1 envelope", case.name));
		assert_eq!(record.stamp(), u64::from(case.generation), "{}", case.name);
		assert_eq!(record.is_edge(), case.flags.is_edge(), "{}", case.name);
		let row = PackedRow::new(&record, packed).unwrap_or_else(|error| {
			panic!(
				"BACKWARDS COMPATIBILITY BROKEN: the frozen row {} is refused: {error:?}",
				case.name
			)
		});
		let decoded = row.decode(None).unwrap_or_else(|error| {
			panic!(
				"BACKWARDS COMPATIBILITY BROKEN: the frozen row {} no longer decodes: {error:?}",
				case.name
			)
		});
		let mut kept = case.data.clone();
		let id = kept.0.remove("id");
		assert!(same(&decoded, &kept), "{} decodes to {decoded:?}", case.name);
		if let Some(id) = id {
			let merged = row.decode(Some(id)).expect("decodes with its id");
			assert!(same(&merged, &case.data), "{} merges its id to {merged:?}", case.name);
		}
		let layout = Arc::new(case.layout.clone());
		let row = PackedRow::new(&record, packed_of(&layout)).expect("a row");
		check_fields(&layout, &row, &decoded, &bytes);
	}
}

/// The current code writes each descriptor and each row as its frozen bytes.
#[test]
fn a_row_packed_now_is_its_frozen_bytes() {
	for case in cases() {
		let (layout, row) = frozen(case.name);
		let encoded = case.layout.kv_canonical().expect("a descriptor encodes");
		assert_eq!(
			hex(&encoded),
			hex(&layout),
			"the descriptor of {} is written differently",
			case.name
		);
		let packed = packed_of(&case.layout);
		match pack(packed, generation(&case), case.flags, &case.data).expect("packs") {
			Packing::Packed(bytes) => {
				assert_eq!(hex(&bytes), hex(&row), "{} is written differently", case.name)
			}
			Packing::Misfit => panic!("{} does not pack", case.name),
		}
	}
}

/// The frozen rows reach what they are said to: escapes, both remainders, every end width but
/// the 4-byte one, an edge, and a multi-byte generation.
#[test]
fn the_frozen_rows_cover_their_shapes() {
	let rows: Vec<(String, Vec<u8>)> =
		cases().iter().map(|case| (case.name.to_owned(), frozen(case.name).1)).collect();
	let header = |name: &str| {
		let (_, bytes) = rows.iter().find(|(n, _)| n == name).expect("a case");
		bytes[1]
	};
	assert_ne!(header("escapes") & StoredRecord::ESCAPES, 0);
	assert_eq!(header("two_byte_ends") & StoredRecord::LAYOUT, StoredRecord::PACKED_U16);
	assert_ne!(header("edge_at_generation_300") & StoredRecord::EDGE, 0);
	let kinds: Vec<PhysicalKind> = cases()
		.iter()
		.flat_map(|case| packed_of(&case.layout).columns.clone())
		.map(|column| column.kind)
		.collect();
	for kind in super::LEAF_KINDS.iter().chain([&PhysicalKind::Object]) {
		assert!(
			kinds.iter().any(|k| super::kind_name(*k) == super::kind_name(*kind)),
			"no frozen row has a {kind:?} column"
		);
	}
}

/// Prints [`FROZEN`]. Ignored so it never runs in CI:
///
/// ```text
/// cargo test -p surrealdb-catalog --lib packed::tests::fixtures::generate -- --ignored --nocapture
/// ```
#[test]
#[ignore = "prints the frozen fixtures; run by hand when a case is added"]
fn generate() {
	fn lines(hex: &str) -> String {
		let chunks: Vec<&str> = hex
			.as_bytes()
			.chunks(64)
			.map(|chunk| std::str::from_utf8(chunk).expect("hex"))
			.collect();
		chunks.join("\\\n\t\t")
	}
	let mut out = String::from("const FROZEN: &[(&str, &str, &str)] = &[\n");
	for case in cases() {
		let layout = case.layout.kv_canonical().expect("encodes");
		let packed = packed_of(&case.layout);
		let Packing::Packed(row) =
			pack(packed, generation(&case), case.flags, &case.data).expect("packs")
		else {
			panic!("{} does not pack", case.name);
		};
		out.push_str(&format!(
			"\t(\n\t\t\"{}\",\n\t\t\"{}\",\n\t\t\"{}\",\n\t),\n",
			case.name,
			lines(&hex(&layout)),
			lines(&hex(&row))
		));
	}
	out.push_str("];");
	println!("{out}");
}
