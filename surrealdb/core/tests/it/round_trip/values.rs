//! Phase 1 of the plan: the value matrix — every scalar, collection and record-id
//! kind a record can hold, round-tripped and compared as typed values.
//!
//! A restore rebuilds each record by re-parsing text the exporter wrote, so every
//! stored value makes a round trip out through `ToSql` and back through the
//! parser. That renderer is not confined to the dump: it is also the stored text
//! of every permission clause, `VALUE`/`ASSERT`/`COMPUTED` expression and view
//! body, and the text `INFO FOR …` reports. A value it renders ambiguously, or
//! renders in a form the parser reads back as something else, therefore corrupts
//! a catalog definition as readily as it loses a field.
//!
//! Each fixture isolates one kind so a failure names what moved rather than
//! showing a diff of a hundred fields, and each runs [`RoundTrip::assert_all`],
//! which compares the records as typed values, the catalog, and the two exports.
//!
//! Two habits recur. Where the *type* of a value is the point, the fixture asks
//! `type::is_*` rather than writing the value down — a comparison against a
//! written `1` is satisfied by `1f` and `1dec` alike. And where the fixture could
//! itself be a lie — a bound the seed's own parser might have clamped, a depth it
//! might not have reached — an absolute assertion against the restored database
//! says what the value has to be, because the round-trip assertions on their own
//! only establish that the two databases agree.

use anyhow::{Result, bail};
use surrealdb_types::{ToSql, Value};

use super::RoundTrip;

/// Asserts the restored database answers `sql` with exactly `expected`.
///
/// The comparison is on rendered SurrealQL, which separates `1`, `1f` and `1dec`
/// and separates `0f` from `-0f` — the distinctions these fixtures exist to pin.
/// This is the absolute half of a test: [`RoundTrip::assert_all`] establishes that
/// the two databases agree, and this establishes what they agree on.
async fn assert_target_renders(rt: &RoundTrip, sql: &str, expected: &str) -> Result<()> {
	let values = rt.target_query(sql).await?;
	let [value] = values.as_slice() else {
		bail!("`{sql}` must answer with exactly one statement, got {}", values.len());
	};
	assert_eq!(value.to_sql(), expected, "the restored database answered `{sql}` with {value:?}");
	Ok(())
}

/// Asserts the restored database answers `sql` with the boolean `true`.
///
/// Used for the `type::is_*` and ordering probes, where the question is a
/// predicate rather than a rendering.
async fn assert_target_true(rt: &RoundTrip, sql: &str) -> Result<()> {
	let values = rt.target_query(sql).await?;
	let [value] = values.as_slice() else {
		bail!("`{sql}` must answer with exactly one statement, got {}", values.len());
	};
	assert_eq!(value, &Value::Bool(true), "the restored database answered `{sql}` with {value:?}");
	Ok(())
}

/// Both ends of the signed 64-bit range survive.
///
/// The minimum is the asymmetric one: its magnitude does not fit in an `i64`, so
/// it can only be read as a literal and not as a negated positive, and a parser
/// that takes the second route overflows or saturates. Every other fixture here
/// uses small numbers, so neither bound is reachable from any of them.
#[tokio::test]
async fn integer_bounds_survive() -> Result<()> {
	let round_trip = RoundTrip::run(
		"CREATE num:bounds SET
			min = -9223372036854775808,
			max = 9223372036854775807;",
	)
	.await?;

	round_trip.assert_all().await?;
	assert_target_renders(
		&round_trip,
		"RETURN [(num:bounds).min, (num:bounds).max]",
		"[-9223372036854775808, 9223372036854775807]",
	)
	.await?;
	assert_target_true(
		&round_trip,
		"RETURN type::is_int((num:bounds).min) AND type::is_int((num:bounds).max)",
	)
	.await
}

/// The extremes of the `f64` range survive.
///
/// A float is rendered in full positional notation rather than with an exponent,
/// so these are the values whose text is longest and whose final digits decide
/// whether the parser lands back on the same bit pattern: the largest finite
/// float, the smallest normal one, and the smallest subnormal. Losing a digit is
/// silent in both directions — the restored record still holds a plausible float.
#[tokio::test]
async fn float_extremes_survive() -> Result<()> {
	let round_trip = RoundTrip::run(
		"CREATE num:floats SET
			largest = 1.7976931348623157e308f,
			smallest_normal = 2.2250738585072014e-308f,
			smallest_subnormal = 5e-324f;",
	)
	.await?;

	round_trip.assert_all().await?;
	// Overflow and underflow are what a lost digit looks like at these
	// magnitudes: the largest float rounds up to infinity, the subnormal rounds
	// down to zero.
	assert_target_true(
		&round_trip,
		"RETURN (num:floats).largest < math::inf
			AND (num:floats).smallest_subnormal > 0f
			AND (num:floats).smallest_normal > 0f
			AND type::is_float((num:floats).largest)",
	)
	.await
}

/// Negative zero keeps its sign.
///
/// `-0f` and `0f` are one magnitude, so nothing that compares numbers can tell a
/// restore that dropped the sign from one that kept it — only the rendering and
/// the sign bit can, and the sign bit is observable through `math::sign`.
#[tokio::test]
async fn negative_zero_keeps_its_sign() -> Result<()> {
	let round_trip = RoundTrip::run("CREATE num:zero SET neg = -0.0f, pos = 0.0f;").await?;

	round_trip.assert_all().await?;
	assert_target_renders(&round_trip, "RETURN [(num:zero).neg, (num:zero).pos]", "[-0f, 0f]")
		.await?;
	assert_target_true(
		&round_trip,
		"RETURN math::sign((num:zero).neg) < 0 AND math::sign((num:zero).pos) > 0",
	)
	.await?;
	// And the two really are one magnitude, so the assertion above is reading the
	// sign rather than a difference it could have found in the value.
	assert_target_true(&round_trip, "RETURN (num:zero).neg = (num:zero).pos").await
}

/// A decimal keeps every significant digit.
///
/// A decimal carries more significant digits than an `f64` can represent, so a
/// restore that rendered one as a float, or re-read it as one, truncates in a way
/// no other fixture's numbers are wide enough to expose. Both ends matter: the
/// widest fraction and the largest magnitude the type holds.
#[tokio::test]
async fn decimal_precision_survives() -> Result<()> {
	let round_trip = RoundTrip::run(
		"CREATE num:decimals SET
			fraction = 1.234567890123456789012345678dec,
			largest = 79228162514264337593543950335dec,
			negative = -0.000000000000000000000000001dec;",
	)
	.await?;

	round_trip.assert_all().await?;
	assert_target_renders(
		&round_trip,
		"RETURN [(num:decimals).fraction, (num:decimals).largest, (num:decimals).negative]",
		"[1.234567890123456789012345678dec, 79228162514264337593543950335dec, \
		 -0.000000000000000000000000001dec]",
	)
	.await?;
	// A float cannot hold either end, so arriving at one is the truncation this
	// fixture exists to catch.
	assert_target_true(
		&round_trip,
		"RETURN type::is_decimal((num:decimals).fraction)
			AND type::is_decimal((num:decimals).largest)
			AND type::is_decimal((num:decimals).negative)",
	)
	.await
}

/// The three number kinds stay distinct as kinds, not merely as magnitudes.
///
/// One magnitude written three ways is the case a magnitude comparison cannot
/// see: `1` equals `1f` equals `1dec` under SurrealQL's own `=`, so a restore
/// that collapsed every number onto one kind satisfies any assertion written as a
/// number. The kind is what arithmetic and index ordering depend on, so the kind
/// is what this asks about.
#[tokio::test]
async fn number_kinds_stay_distinct() -> Result<()> {
	let round_trip = RoundTrip::run("CREATE num:kinds SET i = 1, f = 1f, d = 1dec;").await?;

	round_trip.assert_all().await?;
	assert_target_true(
		&round_trip,
		"RETURN type::is_int((num:kinds).i)
			AND type::is_float((num:kinds).f)
			AND type::is_decimal((num:kinds).d)",
	)
	.await?;
	// And the three really are one magnitude, so the assertion above is reading
	// the kind rather than a difference it could have found in the value.
	assert_target_true(
		&round_trip,
		"RETURN (num:kinds).i = (num:kinds).f AND (num:kinds).f = (num:kinds).d",
	)
	.await
}

/// Strings survive the characters that would end a string literal or a statement.
///
/// Each of these has to be escaped on the way out and unescaped on the way in,
/// and the two failures are not symmetric: an unescaped quote or backslash ends
/// the literal early and the remainder of the record is parsed as SurrealQL,
/// while a doubly-escaped one restores a longer string than was stored. The
/// exporter chooses its quote character per string, so the fixture holds strings
/// that need each one and a string that needs both at once.
#[tokio::test]
async fn strings_survive_quotes_and_escapes() -> Result<()> {
	let round_trip = RoundTrip::run(
		r#"CREATE text:escapes SET
			single = 'it\'s',
			double = "she said \"hi\"",
			both = 'it\'s a "quote"',
			backtick = 'a ` backtick',
			backslash = 'a \\ backslash',
			trailing_backslash = 'ends with \\',
			statement_end = 'a; DEFINE USER hax ON ROOT PASSWORD \'p\' ROLES OWNER; -- ';"#,
	)
	.await?;

	round_trip.assert_all().await?;
	assert_target_renders(
		&round_trip,
		"RETURN [
			string::len((text:escapes).single),
			string::len((text:escapes).double),
			string::len((text:escapes).both),
			string::len((text:escapes).backslash),
			string::len((text:escapes).trailing_backslash),
			string::len((text:escapes).statement_end)
		 ]",
		"[4, 13, 14, 13, 11, 56]",
	)
	.await?;
	// The dump carries that last string as data. Replaying it must not have run
	// what it spells.
	let root = round_trip.target_query("INFO FOR ROOT").await?;
	let Value::Object(root) = &root[0] else {
		bail!("`INFO FOR ROOT` must answer with an object, got {:?}", root[0]);
	};
	let Some(Value::Object(users)) = root.get("users") else {
		bail!("`INFO FOR ROOT` must carry a `users` block, got {root:?}");
	};
	assert!(
		!users.contains_key("hax"),
		"a string stored in a record escaped its literal and defined a root user: {:?}",
		users.keys().collect::<Vec<_>>(),
	);
	Ok(())
}

/// Strings survive the control characters that break a line-oriented reader.
///
/// A dump is newline-separated text, so a stored newline, carriage return or tab
/// is where a record's own content looks like the dump's structure. Normalising a
/// line ending — a stored `\r\n` becoming `\n`, a lone `\r` disappearing — changes
/// the string while leaving a well-formed record behind, which no structural
/// check notices. The NUL byte is the other end of the same problem: it terminates
/// a string for anything that goes through a C-style representation.
#[tokio::test]
async fn strings_survive_control_characters() -> Result<()> {
	let round_trip = RoundTrip::run(
		r"CREATE text:control SET
			tab = 'a\tb',
			newline = 'a\nb',
			carriage_return = 'a\rb',
			crlf = 'a\r\nb',
			nul = 'a\u{0000}b',
			empty = '';",
	)
	.await?;

	round_trip.assert_all().await?;
	// Character counts, because a normalised line ending is a length change and
	// nothing else: `a\r\nb` and `a\nb` both restore as legible text.
	assert_target_renders(
		&round_trip,
		"RETURN [
			string::len((text:control).tab),
			string::len((text:control).newline),
			string::len((text:control).carriage_return),
			string::len((text:control).crlf),
			string::len((text:control).nul),
			string::len((text:control).empty)
		 ]",
		"[3, 3, 3, 4, 3, 0]",
	)
	.await?;
	assert_target_true(
		&round_trip,
		"RETURN (text:control).crlf != (text:control).newline
			AND (text:control).carriage_return != (text:control).newline
			AND type::is_string((text:control).empty)",
	)
	.await
}

/// Strings survive astral-plane characters and the Unicode line separators.
///
/// An astral character is four UTF-8 bytes and two UTF-16 code units, so a
/// restore that re-encoded through a narrower representation splits it into
/// replacement characters — while still producing a string of the right shape.
/// `U+2028`, `U+2029` and `U+0085` are the opposite hazard: they are line
/// terminators to some readers and ordinary characters to SurrealQL, and the
/// exporter writes them raw rather than escaped, so they are the characters most
/// likely to be rewritten by anything that treats a dump as a sequence of lines.
#[tokio::test]
async fn strings_survive_astral_characters_and_line_separators() -> Result<()> {
	let round_trip = RoundTrip::run(
		r"CREATE text:unicode SET
			astral = '🚀🧬',
			combining = 'e\u{0301}',
			line_separator = 'a\u{2028}b',
			paragraph_separator = 'a\u{2029}b',
			next_line = 'a\u{0085}b',
			vertical_tab = 'a\u{000B}b',
			form_feed = 'a\u{000C}b';",
	)
	.await?;

	round_trip.assert_all().await?;
	// Two characters, eight bytes: a re-encoding that lost the pair would land on
	// a different count for one or the other.
	assert_target_renders(
		&round_trip,
		"RETURN [
			string::len((text:unicode).astral),
			array::len(<array><bytes>(text:unicode).astral)
		 ]",
		"[2, 8]",
	)
	.await?;
	// One character each side of the separator plus the separator itself: a
	// reader that split on one would leave a shorter string behind.
	assert_target_renders(
		&round_trip,
		"RETURN [
			string::len((text:unicode).line_separator),
			string::len((text:unicode).paragraph_separator),
			string::len((text:unicode).next_line),
			string::len((text:unicode).vertical_tab),
			string::len((text:unicode).form_feed),
			string::len((text:unicode).combining)
		 ]",
		"[3, 3, 3, 3, 3, 2]",
	)
	.await
}

/// A datetime keeps its nanoseconds.
///
/// A datetime is rendered as text and re-parsed, and the sub-second field is the
/// part a narrower intermediate drops silently: milliseconds for a JSON-shaped
/// representation, microseconds for most database time types. The restored value
/// is still the right instant to the second, so only the digits say otherwise.
#[tokio::test]
async fn datetimes_keep_nanosecond_precision() -> Result<()> {
	let round_trip = RoundTrip::run(
		"CREATE moment:precision SET
			nanos = d'1984-07-03T08:30:45.123456789Z',
			micros = d'1984-07-03T08:30:45.123456Z',
			millis = d'1984-07-03T08:30:45.123Z',
			whole = d'1984-07-03T08:30:45Z';",
	)
	.await?;

	round_trip.assert_all().await?;
	assert_target_renders(
		&round_trip,
		"RETURN [
			time::nano((moment:precision).nanos) - time::nano((moment:precision).whole),
			time::nano((moment:precision).micros) - time::nano((moment:precision).whole),
			time::nano((moment:precision).millis) - time::nano((moment:precision).whole)
		 ]",
		"[123456789, 123456000, 123000000]",
	)
	.await
}

/// Datetimes before the epoch, and at each end of the four-digit year range,
/// survive.
///
/// Every other fixture's timestamps are positive offsets from 1970. A negative
/// one is where a representation holding an unsigned count stops round-tripping,
/// and the year-one instant is the one whose rendering needs its leading zeros to
/// survive the parser.
#[tokio::test]
async fn datetimes_survive_before_the_epoch() -> Result<()> {
	let round_trip = RoundTrip::run(
		"CREATE moment:range SET
			just_before = d'1969-12-31T23:59:59.999999999Z',
			epoch = d'1970-01-01T00:00:00Z',
			year_one = d'0001-01-01T00:00:00Z',
			far_future = d'9999-12-31T23:59:59.999999999Z';",
	)
	.await?;

	round_trip.assert_all().await?;
	assert_target_true(
		&round_trip,
		"RETURN (moment:range).year_one < (moment:range).just_before
			AND (moment:range).just_before < (moment:range).epoch
			AND (moment:range).epoch < (moment:range).far_future",
	)
	.await?;
	assert_target_renders(
		&round_trip,
		"RETURN [time::year((moment:range).year_one), time::year((moment:range).far_future)]",
		"[1, 9999]",
	)
	.await
}

/// A duration keeps every unit it was built from, down to the nanosecond.
///
/// A duration renders as a sum of units and is re-parsed by adding them back up,
/// so the sub-second units are where a narrower intermediate rounds. The zero
/// duration is the separate case: its rendering carries no unit to round.
#[tokio::test]
async fn durations_survive_every_unit() -> Result<()> {
	let round_trip = RoundTrip::run(
		"CREATE span:units SET
			every = 1w2d3h4m5s6ms7us8ns,
			zero = 0s,
			nanosecond = 1ns,
			years = 3y;",
	)
	.await?;

	round_trip.assert_all().await?;
	assert_target_renders(
		&round_trip,
		"RETURN [
			duration::nanos((span:units).every),
			duration::nanos((span:units).zero),
			duration::nanos((span:units).nanosecond)
		 ]",
		// 1w2d3h4m5s6ms7us8ns expressed in nanoseconds.
		"[788645006007008, 0, 1]",
	)
	.await?;
	assert_target_true(
		&round_trip,
		"RETURN type::is_duration((span:units).zero) AND (span:units).years > (span:units).every",
	)
	.await
}

/// The remaining scalar kinds survive as their own kinds.
///
/// A uuid, a byte string and a boolean each render to text another kind could
/// also have produced — a uuid to something a string would spell identically,
/// bytes to a hex literal, a boolean to a bare word. The hazard is therefore not
/// a lost value but a restored value of the wrong kind, which matches nothing
/// the stored value matches and sorts somewhere else in an index.
#[tokio::test]
async fn uuids_bytes_and_booleans_survive() -> Result<()> {
	let round_trip = RoundTrip::run(
		r#"CREATE scalar:other SET
			id_v4 = u'8f2e4a6c-1b3d-4e5f-9a7b-0c1d2e3f4a5b',
			id_v7 = u'019142a4-4a3e-7b4d-9b1e-1b2c3d4e5f60',
			nil = u'00000000-0000-0000-0000-000000000000',
			raw = b"00FF7F",
			no_bytes = b"",
			yes = true,
			no = false;"#,
	)
	.await?;

	round_trip.assert_all().await?;
	assert_target_true(
		&round_trip,
		"RETURN type::is_uuid((scalar:other).id_v4)
			AND type::is_uuid((scalar:other).id_v7)
			AND type::is_uuid((scalar:other).nil)
			AND type::is_bytes((scalar:other).raw)
			AND type::is_bytes((scalar:other).no_bytes)
			AND type::is_bool((scalar:other).yes)
			AND type::is_bool((scalar:other).no)",
	)
	.await?;
	assert_target_renders(
		&round_trip,
		"RETURN [
			<array>(scalar:other).raw,
			array::len(<array>(scalar:other).no_bytes),
			(scalar:other).yes,
			(scalar:other).no
		 ]",
		"[[0, 255, 127], 0, true, false]",
	)
	.await
}

/// `NULL` and `NONE` stay distinct, and each stays where the write path put it.
///
/// They are one absence to most representations and two values here: `NULL` is a
/// stored value, `NONE` is the absence of one. The write path already decides
/// between them — a field or an object entry assigned `NONE` is not stored at
/// all, while an array element assigned `NONE` is — so a restore has to arrive at
/// the same three answers rather than at three of one.
#[tokio::test]
async fn null_and_none_stay_distinct() -> Result<()> {
	let round_trip = RoundTrip::run(
		"CREATE absent:both SET
			null_field = NULL,
			none_field = NONE,
			arr = [NULL, NONE, 1],
			obj = { null_entry: NULL, none_entry: NONE };",
	)
	.await?;

	round_trip.assert_all().await?;
	assert_target_renders(
		&round_trip,
		"RETURN [
			(absent:both).null_field,
			(absent:both).none_field,
			(absent:both).arr,
			(absent:both).obj
		 ]",
		"[NULL, NONE, [NULL, NONE, 1], { null_entry: NULL }]",
	)
	.await?;
	assert_target_true(
		&round_trip,
		"RETURN type::is_null((absent:both).null_field)
			AND type::is_none((absent:both).none_field)
			AND type::is_null((absent:both).arr[0])
			AND type::is_none((absent:both).arr[1])",
	)
	.await
}

/// An empty array and an empty object survive as themselves.
///
/// Emptiness is the shape most easily confused with absence: a restore that read
/// `[]` or `{}` back as `NONE`, or dropped the field holding one, leaves a record
/// that still parses and still answers most queries. Every other container
/// fixture holds something, so none of them can see it.
#[tokio::test]
async fn empty_containers_survive() -> Result<()> {
	let round_trip = RoundTrip::run(
		"CREATE shape:empty SET
			arr = [],
			obj = {},
			nested = { inner: [], items: [[], {}] },
			text = '';",
	)
	.await?;

	round_trip.assert_all().await?;
	assert_target_renders(
		&round_trip,
		"RETURN [(shape:empty).arr, (shape:empty).obj, (shape:empty).nested, (shape:empty).text]",
		"[[], {  }, { inner: [], items: [[], {  }] }, '']",
	)
	.await?;
	assert_target_true(
		&round_trip,
		"RETURN type::is_array((shape:empty).arr)
			AND type::is_object((shape:empty).obj)
			AND type::is_string((shape:empty).text)",
	)
	.await
}

/// A value nesting arrays and objects deeper than one written literal may go
/// survives.
///
/// The storage layer accepts values deeper than a single literal in a statement
/// may be, which is why this fixture builds its depth by repeated update rather
/// than by writing it out. The exporter then has to render that depth and the
/// importer has to accept it back: the two limits are set independently, so a
/// dump can be produced that the restore cannot parse, and the record simply does
/// not arrive. Alternating the two container kinds means neither is trusted to
/// carry the other's nesting.
#[tokio::test]
async fn deeply_nested_mixed_containers_survive() -> Result<()> {
	// Each round wraps the value in an object inside an array, so this many
	// rounds stands for twice as many levels of nesting.
	const ROUNDS: usize = 16;

	let mut seed = String::from("CREATE deep:mixed SET value = 'leaf' RETURN NONE;\n");
	for _ in 0..ROUNDS {
		seed.push_str("UPDATE deep:mixed SET value = [{ inner: value }] RETURN NONE;\n");
	}
	let round_trip = RoundTrip::run(&seed).await?;

	round_trip.assert_all().await?;
	// The fixture has to be as deep as it claims, or the assertions above are
	// comparing two shallow records.
	assert_eq!(
		round_trip.export_a().matches("inner: ").count(),
		ROUNDS,
		"the export must carry all {ROUNDS} levels:\n{}",
		round_trip.export_a(),
	);
	let mut path = String::from("RETURN (deep:mixed).value");
	for _ in 0..ROUNDS {
		path.push_str("[0].inner");
	}
	assert_target_renders(&round_trip, &path, "'leaf'").await
}

/// Ranges survive with both bounds and both bound kinds intact.
///
/// A range is several shapes in one — each end inclusive, exclusive or absent —
/// and the difference between them is a character or two of rendering that no
/// other fixture produces. A restore that read `1>..5` as `1..5`, or an unbounded
/// end as an inclusive one, silently changes which records a query over that
/// range returns.
#[tokio::test]
async fn ranges_survive_their_bound_kinds() -> Result<()> {
	let round_trip = RoundTrip::run(
		"CREATE span:ranges SET
			closed = 1..=5,
			half_open = 1..5,
			exclusive_start = 1>..5,
			exclusive_both = 1>..5,
			open_end = 1..,
			open_start = ..=5,
			unbounded = ..,
			of_strings = 'a'..'z',
			of_datetimes = d'1970-01-01T00:00:00Z'..d'1971-01-01T00:00:00Z';",
	)
	.await?;

	round_trip.assert_all().await?;
	assert_target_renders(
		&round_trip,
		"RETURN [
			(span:ranges).closed, (span:ranges).half_open, (span:ranges).exclusive_start,
			(span:ranges).open_end, (span:ranges).open_start, (span:ranges).unbounded,
			(span:ranges).of_strings
		 ]",
		"[1..=5, 1..5, 1>..5, 1.., ..=5, .., 'a'..'z']",
	)
	.await?;
	// The bound kinds are observable, not merely rendered: an inclusive end
	// admits its endpoint and an exclusive start excludes its own.
	assert_target_true(
		&round_trip,
		"RETURN 5 IN (span:ranges).closed
			AND !(5 IN (span:ranges).half_open)
			AND !(1 IN (span:ranges).exclusive_start)
			AND type::is_range((span:ranges).unbounded)",
	)
	.await
}

/// Geometries survive as geometries, with their coordinates intact.
///
/// A geometry renders in two shapes — a point as a coordinate pair, a polygon as
/// a `type`/`coordinates` object — and only the second is what the value looks
/// like in JSON. Both have to be read back as geometry rather than as the tuple
/// or the object they resemble, because geometry is what the spatial functions
/// and the spatial index accept.
#[tokio::test]
async fn geometries_survive() -> Result<()> {
	let round_trip = RoundTrip::run(
		"CREATE place:geo SET
			point = (-0.118092, 51.509865),
			integral_point = (1, 2),
			polygon = {
				type: 'Polygon',
				coordinates: [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]]
			};",
	)
	.await?;

	round_trip.assert_all().await?;
	assert_target_true(
		&round_trip,
		"RETURN type::is_geometry((place:geo).point)
			AND type::is_geometry((place:geo).integral_point)
			AND type::is_geometry((place:geo).polygon)",
	)
	.await?;
	assert_target_renders(
		&round_trip,
		"RETURN [(place:geo).point, (place:geo).integral_point]",
		"[(-0.118092f, 51.509865f), (1f, 2f)]",
	)
	.await?;
	// A geometry holds floats whatever its literal was written as, so the restore
	// has to arrive at floats too — including where the exporter has rendered a
	// coordinate without the suffix that would say so.
	assert_target_true(
		&round_trip,
		"RETURN type::is_float((place:geo).polygon.coordinates[0][1][0])
			AND type::is_float((place:geo).integral_point.coordinates[0])",
	)
	.await
}

/// An object shaped like a geometry stays on the side of the line it started on.
///
/// `type` plus `coordinates` and nothing else is read as a geometry; the same
/// object with one more entry stays an object. One entry decides it, and both
/// directions are silent — an object promoted to a geometry loses the freedom to
/// hold anything else, a geometry demoted to an object stops answering the
/// spatial functions — so only a fixture holding both shapes at once can say
/// which way a restore moved.
#[tokio::test]
async fn a_geometry_shaped_object_keeps_its_side_of_the_line() -> Result<()> {
	let round_trip = RoundTrip::run(
		"CREATE place:line SET
			exactly = { type: 'Point', coordinates: [1.0, 2.0] },
			one_more_entry = { type: 'Point', coordinates: [1.0, 2.0], note: 'an object' },
			wrong_type = { type: 'NotAGeometry', coordinates: [1.0, 2.0] },
			no_coordinates = { type: 'Point' };",
	)
	.await?;

	round_trip.assert_all().await?;
	assert_target_true(
		&round_trip,
		"RETURN type::is_geometry((place:line).exactly)
			AND type::is_object((place:line).one_more_entry)
			AND type::is_object((place:line).wrong_type)
			AND type::is_object((place:line).no_coordinates)",
	)
	.await?;
	// The two are one entry apart and must not have converged on each other.
	assert_target_true(&round_trip, "RETURN (place:line).exactly != (place:line).one_more_entry")
		.await
}

/// Every kind of record id survives, as its own kind.
///
/// A record id is rendered into the `INSERT` that restores its record, so an id
/// the exporter mis-renders does not corrupt a field: it addresses a different
/// record, or fails to parse and loses the record outright. The pairs here are
/// the ones that render alike unless the id keeps its kind — a number against a
/// string of digits, a uuid against a string that spells one — and the array and
/// object ids are the kinds that nest a whole value inside the id.
#[tokio::test]
async fn record_ids_of_every_kind_survive() -> Result<()> {
	let round_trip = RoundTrip::run(
		r"CREATE ident:1 SET kind = 'number';
		  CREATE ident:-9223372036854775808 SET kind = 'number at the bound';
		  CREATE ident:`0123` SET kind = 'string of digits';
		  CREATE ident:`has space` SET kind = 'string needing escapes';
		  CREATE ident:`it\'s a ; -- \n` SET kind = 'string of punctuation';
		  CREATE ident:u'019142a4-4a3e-7b4d-9b1e-1b2c3d4e5f60' SET kind = 'uuid';
		  CREATE ident:`019142a4-4a3e-7b4d-9b1e-1b2c3d4e5f60` SET kind = 'string spelling a uuid';
		  CREATE ident:['a', 1, NULL] SET kind = 'array';
		  CREATE ident:{ x: 1, y: 'two' } SET kind = 'object';",
	)
	.await?;

	round_trip.assert_all().await?;
	// Nine records, so no two ids collapsed onto each other on the way through.
	assert_target_renders(&round_trip, "RETURN count(SELECT id FROM ident)", "9").await?;
	// The pairs that render alike unless the id keeps its kind.
	assert_target_true(
		&round_trip,
		"RETURN (ident:1).id != (ident:`0123`).id
			AND (ident:u'019142a4-4a3e-7b4d-9b1e-1b2c3d4e5f60').id
				!= (ident:`019142a4-4a3e-7b4d-9b1e-1b2c3d4e5f60`).id
			AND (ident:['a', 1, NULL]).kind = 'array'
			AND (ident:{ x: 1, y: 'two' }).kind = 'object'",
	)
	.await
}

/// Table and field names carrying SurrealQL punctuation survive, and stay data.
///
/// A name reaches the dump as identifier text rather than as a quoted value, so
/// it is the one place where a missing escape does not corrupt a record but
/// appends a statement: the name closes its own quoting and its remainder is
/// parsed and executed with the restore's authority. Every name in this corpus is
/// built to do exactly that — each ends its own backticks and continues with a
/// statement, one of them a root user grant — so the test asserts both that the
/// names round-trip and that nothing inside one ran.
#[tokio::test]
async fn adversarial_table_and_field_names_survive() -> Result<()> {
	let round_trip = RoundTrip::run(
		r#"define table if not exists `pwnme666\`;\ncreate cats666 set aaaaaa=1;--`;
		   create `pwnme667\`;\ncreate meow666 set aaaaaa=1;--` set name="hello1";
		   relate person:`a`->`friends\`;\ncreate meow set meow='yaaay';--`->person:b set meow='meow';

		   define table bla;
		   DEFINE FIELD ` ads\\\\` ON TABLE bla TYPE number;
		   DEFINE FIELD ` on table bla type number; define table hax -- ` ON TABLE bla TYPE number;
		   DEFINE FIELD `it\'s "quoted";\nDEFINE USER hax ON ROOT PASSWORD \'p\' ROLES OWNER;--` ON TABLE bla TYPE option<string>;
		   CREATE bla:1 SET
			 ` ads\\\\` = 1,
			 ` on table bla type number; define table hax -- ` = 2,
			 `it\'s "quoted";\nDEFINE USER hax ON ROOT PASSWORD \'p\' ROLES OWNER;--` = 'a value';

		   relate person:`a`->`friends2\`;\nDEFINE USER IF NOT EXISTS pwned ON ROOT PASSWORD 'pwned' ROLES OWNER DURATION FOR SESSION 12h, FOR TOKEN 1m;--`->person:b set meow='meow';"#,
	)
	.await?;

	round_trip.assert_all().await?;

	// Every statement the corpus tries to smuggle reaches the dump as part of a
	// name, so the restore really did parse these names rather than skip them.
	for fragment in ["create cats666", "create meow666", "define table hax", "DEFINE USER"] {
		assert!(
			round_trip.export_a().contains(fragment),
			"the export must carry `{fragment}` as part of a name:\n{}",
			round_trip.export_a(),
		);
	}

	// None of them ran: no table or user a smuggled statement would have created
	// exists on the restored side.
	let db = round_trip.target_query("INFO FOR DB").await?;
	let Value::Object(db) = &db[0] else {
		bail!("`INFO FOR DB` must answer with an object, got {:?}", db[0]);
	};
	let Some(Value::Object(tables)) = db.get("tables") else {
		bail!("`INFO FOR DB` must carry a `tables` block, got {db:?}");
	};
	for smuggled in ["cats666", "meow666", "meow", "hax"] {
		assert!(
			!tables.contains_key(smuggled),
			"a name in the dump escaped its quoting and defined table `{smuggled}`: {:?}",
			tables.keys().collect::<Vec<_>>(),
		);
	}
	let root = round_trip.target_query("INFO FOR ROOT").await?;
	let Value::Object(root) = &root[0] else {
		bail!("`INFO FOR ROOT` must answer with an object, got {:?}", root[0]);
	};
	let Some(Value::Object(users)) = root.get("users") else {
		bail!("`INFO FOR ROOT` must carry a `users` block, got {root:?}");
	};
	for smuggled in ["hax", "pwned"] {
		assert!(
			!users.contains_key(smuggled),
			"a name in the dump escaped its quoting and defined root user `{smuggled}`: {:?}",
			users.keys().collect::<Vec<_>>(),
		);
	}
	Ok(())
}
