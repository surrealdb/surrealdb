mod helpers;
mod parse;
use helpers::Test;
use surrealdb::err::Error;

#[tokio::test]
async fn idiom_chain_part_optional() -> Result<(), Error> {
	let sql = r#"
		{}.prop.is_bool();
		{}.prop?.is_bool();
	"#;
	Test::new(sql).await?.expect_val("false")?.expect_val("None")?;
	Ok(())
}

#[tokio::test]
async fn idiom_index_expression() -> Result<(), Error> {
	let sql = r#"
		[1,2,3,4][1 + 1];
	"#;
	Test::new(sql).await?.expect_val("3")?;
	Ok(())
}

#[tokio::test]
async fn idiom_index_call() -> Result<(), Error> {
	let sql = r#"
		DEFINE FUNCTION fn::foo() {
			return 1 + 1;
		};
		RETURN [1,2,3,4][fn::foo()];
	"#;
	Test::new(sql).await?.expect_val("None")?.expect_val("3")?;
	Ok(())
}

#[tokio::test]
async fn idiom_index_range() -> Result<(), Error> {
	let sql = r#"
		[1,2,3,4][1..2];
		[1,2,3,4][1..=2];
		[1,2,3,4][1>..=2];
		[1,2,3,4][1>..];
		[1,2,3,4][1..];
		[1,2,3,4][..2];
		[1,2,3,4][..=2];
	"#;
	Test::new(sql)
		.await?
		.expect_val("[2]")?
		.expect_val("[2,3]")?
		.expect_val("[3]")?
		.expect_val("[3,4]")?
		.expect_val("[2,3,4]")?
		.expect_val("[1,2]")?
		.expect_val("[1,2,3]")?;
	Ok(())
}

#[tokio::test]
async fn idiom_array_nested_prop_continues_as_array() -> Result<(), Error> {
	let sql = r#"
    	[{x:2}].x[0];
    	[{x:2}].x.at(0);
	"#;
	Test::new(sql).await?.expect_val("2")?.expect_val("2")?;
	Ok(())
}

#[tokio::test]
async fn idiom_select_all_from_nested_array_prop() -> Result<(), Error> {
	let sql = r#"
    	CREATE a:1, a:2;
        RELATE a:1->edge:1->a:2;
        a:1->edge.out;
        a:1->edge.out.*;
	"#;
	Test::new(sql)
		.await?
		.expect_val("[{id: a:1}, {id: a:2}]")?
		.expect_val("[{id: edge:1, in: a:1, out: a:2}]")?
		.expect_val("[a:2]")?
		.expect_val("[{id: a:2}]")?;
	Ok(())
}

#[tokio::test]
async fn idiom_graph_with_filter_should_be_flattened() -> Result<(), Error> {
	let sql = r#"
    	CREATE person:1, person:2, person:3;
		RELATE person:1->likes:1->person:2;
		RELATE person:2->likes:2->person:3;
		person:1->likes->person->likes->person;
		person:1->likes->person[?true]->likes->person;
		person:1->likes->person[?true];
		[person:1][?true]->likes->person;
		[person:1]->likes->person[?true]->likes->person;
		SELECT ->likes[?true]->person as likes FROM person;
	"#;
	Test::new(sql)
		.await?
		.expect_val("[{id: person:1}, {id: person:2}, {id: person:3}]")?
		.expect_val("[{id: likes:1, in: person:1, out: person:2}]")?
		.expect_val("[{id: likes:2, in: person:2, out: person:3}]")?
		.expect_val("[person:3]")?
		.expect_val("[person:3]")?
		.expect_val("[person:2]")?
		.expect_val("[[person:2]]")?
		.expect_val("[[person:3]]")?
		.expect_val(
			"[
			{likes: [person:2]},
			{likes: [person:3]},
			{likes: []},
		]",
		)?;
	Ok(())
}

#[tokio::test]
async fn idiom_optional_after_value_should_pass_through() -> Result<(), Error> {
	let sql = r#"
		none?;
		null?;
		1?;
		'a'?;
		1s?;
		time::EPOCH?;
		u'0192fb97-e8ee-7683-8198-95710b103bd5'?;
		[]?;
		{}?;
		(89.0, 90.0)?;
		<bytes>"hhehehe"?;
		person:aeon?;
		{
			type: "Polygon",
			coordinates: [[
				[-111.0690, 45.0032],
				[-104.0838, 44.9893],
				[-104.0910, 40.9974],
				[-111.0672, 40.9862]
			]]
		}?;
	"#;
	Test::new(sql)
		.await?
		.expect_val("none")?
		.expect_val("null")?
		.expect_val("1")?
		.expect_val("'a'")?
		.expect_val("1s")?
		.expect_val("d'1970-01-01T00:00:00Z'")?
		.expect_val("u'0192fb97-e8ee-7683-8198-95710b103bd5'")?
		.expect_val("[]")?
		.expect_val("{}")?
		.expect_val("(89.0, 90.0)")?
		.expect_bytes([104, 104, 101, 104, 101, 104, 101])?
		.expect_val("person:aeon")?
		.expect_val(
			"{
			type: 'Polygon',
			coordinates: [[
				[-111.0690, 45.0032],
				[-104.0838, 44.9893],
				[-104.0910, 40.9974],
				[-111.0672, 40.9862]
			]]
		}",
		)?;
	Ok(())
}

#[tokio::test]
async fn idiom_recursion_graph() -> Result<(), Error> {
	let sql = r#"
		INSERT INTO person [
			{ id: person:tobie, name: 'Tobie' },
			{ id: person:jaime, name: 'Jaime' },
			{ id: person:micha, name: 'Micha' },
			{ id: person:john, name: 'John' },
			{ id: person:mary, name: 'Mary' },
			{ id: person:tim, name: 'Tim' },
		];

		INSERT RELATION INTO knows [
			{ id: knows:1, in: person:tobie, out: person:jaime },
			{ id: knows:2, in: person:tobie, out: person:micha },
			{ id: knows:3, in: person:micha, out: person:john },
			{ id: knows:4, in: person:jaime, out: person:mary },
			{ id: knows:5, in: person:mary, out: person:tim },
		];
		
		SELECT name, @{1}(->knows->person).name AS names_1sts FROM person;
		SELECT name, @{2}(->knows->person).name AS names_2nds FROM person;
		SELECT name, @{3}(->knows->person).name AS names_3rds FROM person;

		SELECT VALUE @{..}.{ name, knows: ->knows->person.@ } FROM person;
	"#;
	Test::new(sql)
		.await?
		.expect_val(
			"[
			{ id: person:tobie, name: 'Tobie' },
			{ id: person:jaime, name: 'Jaime' },
			{ id: person:micha, name: 'Micha' },
			{ id: person:john, name: 'John' },
			{ id: person:mary, name: 'Mary' },
			{ id: person:tim, name: 'Tim' },
		]",
		)?
		.expect_val(
			"[
			{ id: knows:1, in: person:tobie, out: person:jaime },
			{ id: knows:2, in: person:tobie, out: person:micha },
			{ id: knows:3, in: person:micha, out: person:john },
			{ id: knows:4, in: person:jaime, out: person:mary },
			{ id: knows:5, in: person:mary, out: person:tim },
		]",
		)?
		.expect_val(
			"[
			{ name: 'Jaime', names_1sts: ['Mary'] },
			{ name: 'John', names_1sts: [] },
			{ name: 'Mary', names_1sts: ['Tim'] },
			{ name: 'Micha', names_1sts: ['John'] },
			{ name: 'Tim', names_1sts: [] },
			{ name: 'Tobie', names_1sts: ['Jaime', 'Micha'] },
		]",
		)?
		.expect_val(
			"[
			{ name: 'Jaime', names_2nds: ['Tim'] },
			{ name: 'John', names_2nds: [] },
			{ name: 'Mary', names_2nds: [] },
			{ name: 'Micha', names_2nds: [] },
			{ name: 'Tim', names_2nds: [] },
			{ name: 'Tobie', names_2nds: ['Mary', 'John'] },
		]",
		)?
		.expect_val(
			"[
			{ name: 'Jaime', names_3rds: [] },
			{ name: 'John', names_3rds: [] },
			{ name: 'Mary', names_3rds: [] },
			{ name: 'Micha', names_3rds: [] },
			{ name: 'Tim', names_3rds: [] },
			{ name: 'Tobie', names_3rds: ['Tim'] },
		]",
		)?
		.expect_val(
			"[
			{
				knows: [{
					knows: [{
						knows: [],
						name: 'Tim'
					}],
					name: 'Mary'
				}],
				name: 'Jaime'
			},
			{
				knows: [],
				name: 'John'
			},
			{
				knows: [{
					knows: [],
					name: 'Tim'
				}],
				name: 'Mary'
			},
			{
				knows: [{
					knows: [],
					name: 'John'
				}],
				name: 'Micha'
			},
			{
				knows: [],
				name: 'Tim'
			},
			{
				knows: [{
					knows: [{
						knows: [{
							knows: [],
							name: 'Tim'
						}],
						name: 'Mary'
					}],
					name: 'Jaime'
				},
				{
					knows: [{
						knows: [],
						name: 'John'
					}],
					name: 'Micha'
				}],
				name: 'Tobie'
			}
		]",
		)?;
	Ok(())
}

#[tokio::test]
async fn idiom_recursion_record_links() -> Result<(), Error> {
	let sql = r#"
		INSERT [
			{ id: planet:earth, 		name: 'Earth', 				contains: [country:us, country:canada] },

			{ id: country:us, 			name: 'United States', 		contains: [state:california, state:texas] },
			{ id: country:canada, 		name: 'Canada', 			contains: [province:ontario, province:bc] },

			{ id: state:california, 	name: 'California', 		contains: [city:los_angeles, city:san_francisco] },
			{ id: state:texas, 			name: 'Texas', 				contains: [city:houston, city:dallas] },
			{ id: province:ontario, 	name: 'Ontario', 			contains: [city:toronto, city:ottawa] },
			{ id: province:bc, 			name: 'British Columbia', 	contains: [city:vancouver, city:victoria] },

			{ id: city:los_angeles, 	name: 'Los Angeles' },
			{ id: city:san_francisco, 	name: 'San Francisco' },
			{ id: city:houston, 		name: 'Houston' },
			{ id: city:dallas, 			name: 'Dallas' },
			{ id: city:toronto, 		name: 'Toronto' },
			{ id: city:ottawa,			name: 'Ottowa' },
			{ id: city:vancouver,		name: 'Vancouver' },
			{ id: city:victoria,		name: 'Victoria' },
		];

		planet:earth.{1}(.contains).name;
		planet:earth.{2}(.contains).name;
		planet:earth.{3}(.contains).name;
		planet:earth.{4}(.contains).name;

		planet:earth.{1}.contains.@;
		planet:earth.{2}.contains.@;
		planet:earth.{3}.contains.@;
		planet:earth.{1}.contains.@.name;
		planet:earth.{2}.contains.@.name;
		planet:earth.{3}.contains.@.name;

		planet:earth.{1}.{ id, name, places: contains.@ };
		planet:earth.{2}.{ id, name, places: contains.@ };
		planet:earth.{3}.{ id, name, places: contains.@ };
		planet:earth.{4}.{ id, name, places: contains.@ };
		planet:earth.{..}.{ id, name, places: contains.@ };
		planet:earth.{5..}.{ id, name, places: contains.@ };

		planet:earth.{..}.{ id, name, places: contains.@.chain(|$v| $v ?? []) };
	"#;
	Test::new(sql)
		.await?
		.expect_val("[
			{ id: planet:earth, 		name: 'Earth', 				contains: [country:us, country:canada] },

			{ id: country:us, 			name: 'United States', 		contains: [state:california, state:texas] },
			{ id: country:canada, 		name: 'Canada', 			contains: [province:ontario, province:bc] },

			{ id: state:california, 	name: 'California', 		contains: [city:los_angeles, city:san_francisco] },
			{ id: state:texas, 			name: 'Texas', 				contains: [city:houston, city:dallas] },
			{ id: province:ontario, 	name: 'Ontario', 			contains: [city:toronto, city:ottawa] },
			{ id: province:bc, 			name: 'British Columbia', 	contains: [city:vancouver, city:victoria] },

			{ id: city:los_angeles, 	name: 'Los Angeles' },
			{ id: city:san_francisco, 	name: 'San Francisco' },
			{ id: city:houston, 		name: 'Houston' },
			{ id: city:dallas, 			name: 'Dallas' },
			{ id: city:toronto, 		name: 'Toronto' },
			{ id: city:ottawa,			name: 'Ottowa' },
			{ id: city:vancouver,		name: 'Vancouver' },
			{ id: city:victoria,		name: 'Victoria' },
		]")?
		.expect_val("[
			'United States',
			'Canada',
		]")?
		.expect_val("[
			'California',
			'Texas',
			'Ontario',
			'British Columbia'
		]")?
		.expect_val("[
			'Los Angeles',
			'San Francisco',
			'Houston',
			'Dallas',
			'Toronto',
			'Ottowa',
			'Vancouver',
			'Victoria'
		]")?
		.expect_val("[]")?
		.expect_val("[
			country:us,
			country:canada,
		]")?
		.expect_val("[
			state:california,
			state:texas,
			province:ontario,
			province:bc,
		]")?
		.expect_val("[
			city:los_angeles,
			city:san_francisco,
			city:houston,
			city:dallas,
			city:toronto,
			city:ottawa,
			city:vancouver,
			city:victoria,
		]")?
		.expect_val("[
			'United States',
			'Canada',
		]")?
		.expect_val("[
			'California',
			'Texas',
			'Ontario',
			'British Columbia'
		]")?
		.expect_val("[
			'Los Angeles',
			'San Francisco',
			'Houston',
			'Dallas',
			'Toronto',
			'Ottowa',
			'Vancouver',
			'Victoria'
		]")?
		.expect_val("{
			id: planet:earth,
			name: 'Earth',
			places: [
				country:us,
				country:canada
			]
		}")?
		.expect_val("{
			id: planet:earth,
			name: 'Earth',
			places: [
				{
					id: country:us,
					name: 'United States',
					places: [
						state:california,
						state:texas
					]
				},
				{
					id: country:canada,
					name: 'Canada',
					places: [
						province:ontario,
						province:bc
					]
				}
			]
		}")?
		.expect_val("{
			id: planet:earth,
			name: 'Earth',
			places: [
				{
					id: country:us,
					name: 'United States',
					places: [
						{
							id: state:california,
							name: 'California',
							places: [
								city:los_angeles,
								city:san_francisco
							]
						},
						{
							id: state:texas,
							name: 'Texas',
							places: [
								city:houston,
								city:dallas
							]
						}
					]
				},
				{
					id: country:canada,
					name: 'Canada',
					places: [
						{
							id: province:ontario,
							name: 'Ontario',
							places: [
								city:toronto,
								city:ottawa
							]
						},
						{
							id: province:bc,
							name: 'British Columbia',
							places: [
								city:vancouver,
								city:victoria
							]
						}
					]
				}
			]
		}")?
		.expect_val("{
			id: planet:earth,
			name: 'Earth',
			places: [
				{
					id: country:us,
					name: 'United States',
					places: [
						{
							id: state:california,
							name: 'California',
							places: [
								{
									id: city:los_angeles,
									name: 'Los Angeles',
									places: NONE
								},
								{
									id: city:san_francisco,
									name: 'San Francisco',
									places: NONE
								}
							]
						},
						{
							id: state:texas,
							name: 'Texas',
							places: [
								{
									id: city:houston,
									name: 'Houston',
									places: NONE
								},
								{
									id: city:dallas,
									name: 'Dallas',
									places: NONE
								}
							]
						}
					]
				},
				{
					id: country:canada,
					name: 'Canada',
					places: [
						{
							id: province:ontario,
							name: 'Ontario',
							places: [
								{
									id: city:toronto,
									name: 'Toronto',
									places: NONE
								},
								{
									id: city:ottawa,
									name: 'Ottowa',
									places: NONE
								}
							]
						},
						{
							id: province:bc,
							name: 'British Columbia',
							places: [
								{
									id: city:vancouver,
									name: 'Vancouver',
									places: NONE
								},
								{
									id: city:victoria,
									name: 'Victoria',
									places: NONE
								}
							]
						}
					]
				}
			]
		}")?
		.expect_val("{
			id: planet:earth,
			name: 'Earth',
			places: [
				{
					id: country:us,
					name: 'United States',
					places: [
						{
							id: state:california,
							name: 'California',
							places: [
								{
									id: city:los_angeles,
									name: 'Los Angeles',
									places: NONE
								},
								{
									id: city:san_francisco,
									name: 'San Francisco',
									places: NONE
								}
							]
						},
						{
							id: state:texas,
							name: 'Texas',
							places: [
								{
									id: city:houston,
									name: 'Houston',
									places: NONE
								},
								{
									id: city:dallas,
									name: 'Dallas',
									places: NONE
								}
							]
						}
					]
				},
				{
					id: country:canada,
					name: 'Canada',
					places: [
						{
							id: province:ontario,
							name: 'Ontario',
							places: [
								{
									id: city:toronto,
									name: 'Toronto',
									places: NONE
								},
								{
									id: city:ottawa,
									name: 'Ottowa',
									places: NONE
								}
							]
						},
						{
							id: province:bc,
							name: 'British Columbia',
							places: [
								{
									id: city:vancouver,
									name: 'Vancouver',
									places: NONE
								},
								{
									id: city:victoria,
									name: 'Victoria',
									places: NONE
								}
							]
						}
					]
				}
			]
		}")?
		.expect_val("NONE")?
		.expect_val("{
			id: planet:earth,
			name: 'Earth',
			places: [
				{
					id: country:us,
					name: 'United States',
					places: [
						{
							id: state:california,
							name: 'California',
							places: [
								{
									id: city:los_angeles,
									name: 'Los Angeles',
									places: []
								},
								{
									id: city:san_francisco,
									name: 'San Francisco',
									places: []
								}
							]
						},
						{
							id: state:texas,
							name: 'Texas',
							places: [
								{
									id: city:houston,
									name: 'Houston',
									places: []
								},
								{
									id: city:dallas,
									name: 'Dallas',
									places: []
								}
							]
						}
					]
				},
				{
					id: country:canada,
					name: 'Canada',
					places: [
						{
							id: province:ontario,
							name: 'Ontario',
							places: [
								{
									id: city:toronto,
									name: 'Toronto',
									places: []
								},
								{
									id: city:ottawa,
									name: 'Ottowa',
									places: []
								}
							]
						},
						{
							id: province:bc,
							name: 'British Columbia',
							places: [
								{
									id: city:vancouver,
									name: 'Vancouver',
									places: []
								},
								{
									id: city:victoria,
									name: 'Victoria',
									places: []
								}
							]
						}
					]
				}
			]
		}")?;
	Ok(())
}

#[tokio::test]
async fn idiom_recursion_path_elimination() -> Result<(), Error> {
	let sql = r#"
		INSERT [
			{ id: a:1, name: 'One',   links: [a:2, a:3] },
			{ id: a:2, name: 'Two',   links: [a:3] },
			{ id: a:3, name: 'Three', links: [] },
		];

		a:1.{1}.{ name, links: links.@ };
		a:1.{2}.{ name, links: links.@ };
		a:1.{3}.{ name, links: links.@ };
		a:1.{4}.{ name, links: links.@ };
	"#;
	Test::new(sql)
		.await?
		.expect_val(
			"[
			{ id: a:1, name: 'One',   links: [a:2, a:3] },
			{ id: a:2, name: 'Two',   links: [a:3] },
			{ id: a:3, name: 'Three', links: [] },
		]",
		)?
		.expect_val(
			"{
			name: 'One',
			links: [a:2, a:3],
		}",
		)?
		.expect_val(
			"{
			name: 'One',
			links: [
				{
					name: 'Two',
					links: [a:3]
				},
				{
					name: 'Three',
					links: [],
				}
			],
		}",
		)?
		.expect_val(
			"{
			name: 'One',
			links: [
				{
					name: 'Two',
					links: [
						{
							name: 'Three',
							links: [],
						}
					]
				}
			],
		}",
		)?
		.expect_val("NONE")?;
	Ok(())
}

#[tokio::test]
async fn idiom_recursion_repeat_recurse_nested_destructure() -> Result<(), Error> {
	let sql = r#"
		INSERT [
			{ id: a:1, name: 'One',   links: { a: [a:2, a:3] } },
			{ id: a:2, name: 'Two',   links: { a: [a:3] } },
			{ id: a:3, name: 'Three', links: { a: [] } },
		];

		a:1.{1}.{ name, links.{ a: a.@ } };
		a:1.{2}.{ name, links.{ a: a.@ } };
		a:1.{3}.{ name, links.{ a: a.@ } };
		a:1.{4}.{ name, links.{ a: a.@ } };
	"#;
	Test::new(sql)
		.await?
		.expect_val(
			"[
			{ id: a:1, name: 'One',   links: { a: [a:2, a:3] } },
			{ id: a:2, name: 'Two',   links: { a: [a:3] } },
			{ id: a:3, name: 'Three', links: { a: [] } },
		]",
		)?
		.expect_val(
			"{
			name: 'One',
			links: {
				a: [a:2, a:3]
			},
		}",
		)?
		.expect_val(
			"{
			name: 'One',
			links: {
				a: [
					{
						name: 'Two',
						links: {
							a: [a:3]
						}
					},
					{
						name: 'Three',
						links: {
							a: []
						}
					}
				]
			},
		}",
		)?
		.expect_val(
			"{
			name: 'One',
			links: {
				a: [
					{
						name: 'Two',
						links: {
							a: [
								{
									name: 'Three',
									links: { 
										a: [] 
									}
								}
							]
						}
					}
				]
			},
		}",
		)?
		.expect_val("NONE")?;
	Ok(())
}

#[tokio::test]
async fn idiom_recursion_limits() -> Result<(), Error> {
	let sql = r#"
		FOR $i IN 1..=300 {
			UPSERT type::thing('a', $i) SET link = type::thing('a', $i + 1);
		};

		a:1.{0..}.link;
		a:1.{1..}.link;
		a:1.{..256}.link;
		a:1.{..257}.link;

		a:1.@;
	"#;
	Test::new(sql)
		.await?
		.expect_val("NONE")?
		.expect_error("Found 0 for bound but expected at least 1.")?
		.expect_error("Exceeded the idiom recursion limit of 256.")?
		.expect_val("a:257")?
		.expect_error("Found 257 for bound but expected 256 at most.")?
		.expect_error(
			"Tried to use a `@` repeat recurse symbol in a position where it is not supported",
		)?;
	Ok(())
}

#[tokio::test]
async fn idiom_object_dot_star() -> Result<(), Error> {
	let sql = r#"
		{ a: 1, b: 2 }.*;

		DEFINE FIELD obj ON test TYPE object;
		DEFINE FIELD obj.* ON test TYPE number;
		CREATE test:1 SET obj.a = 'a';

		--------

		DEFINE FIELD emails.address ON TABLE user TYPE string;
		-- Previously `emails.*` would be considered the same as `emails`
		-- Resulting in two conflicting types for the same field. But now 
		-- `emails.*`, for objects, targets all values when `emails` is an object
		-- and all entries when `emails` is an array.
		DEFINE FIELD emails.*.address ON TABLE user TYPE option<number>;
		DEFINE FIELD tags.*.value ON TABLE user TYPE option<string>;

		CREATE user:1 SET emails.address = 9;
		CREATE user:2 SET emails.address = "me@me.com";
		create user:3 set tags = [{ value: 'bla' }], emails.address = "me@me.com";

		--------

		create only person:tobie set name = 'tobie';

		select * from ONLY person:tobie;

		select * from ONLY person:tobie.*;   -- this
		select * from ONLY (person:tobie.*); -- does this
		(select * from ONLY person:tobie).*; -- not this

		select * from ONLY { id: person:tobie, name: 'tobie' };
		select * from { id: person:tobie, name: 'tobie' }.*;
		select * from person:tobie, 'tobie';
		return person:tobie;
		return person:tobie.*;
	"#;
	Test::new(sql)
		.await?
		.expect_val("[1, 2]")?
		.expect_val("NONE")?
		.expect_val("NONE")?
		.expect_error("Found 'a' for field `obj[*]`, with record `test:1`, but expected a number")?
		.expect_val("NONE")?
		.expect_val("NONE")?
		.expect_val("NONE")?
		.expect_error(
			"Found 9 for field `emails.address`, with record `user:1`, but expected a string",
		)?
		.expect_val(
			"[
			{
				emails: {
					address: 'me@me.com'
				},
				id: user:2
			}
		]",
		)?
		.expect_val(
			"[
			{
				emails: {
					address: 'me@me.com'
				},
				id: user:3,
				tags: [
					{
						value: 'bla'
					}
				]
			}
		]",
		)?
		.expect_val(
			"{
			id: person:tobie,
			name: 'tobie'
		}",
		)?
		.expect_val(
			"{
			id: person:tobie,
			name: 'tobie'
		}",
		)?
		.expect_val(
			"{
			id: person:tobie,
			name: 'tobie'
		}",
		)?
		.expect_val(
			"{
			id: person:tobie,
			name: 'tobie'
		}",
		)?
		.expect_val(
			"[
			person:tobie,
			'tobie'
		]",
		)?
		.expect_val(
			"{
			id: person:tobie,
			name: 'tobie'
		}",
		)?
		.expect_val(
			"[
			{
				id: person:tobie,
				name: 'tobie'
			},
			'tobie'
		]",
		)?
		.expect_val(
			"[
			{
				id: person:tobie,
				name: 'tobie'
			},
			'tobie'
		]",
		)?
		.expect_val("person:tobie")?
		.expect_val(
			"{
			id: person:tobie,
			name: 'tobie'
		}",
		)?;
	Ok(())
}

#[tokio::test]
async fn idiom_function_argument_computation() -> Result<(), Error> {
	let sql = r#"
		LET $str = "abc";
		"abcdef".starts_with($str);

		LET $obj = { 
			a: |$a: int| $a,
			b: |$b: function| $b()
		};

		LET $num = 123;
		LET $fnc = || 456;

		$obj.a($num);
		$obj.b($fnc);
	"#;
	Test::new(sql)
		.await?
		.expect_val("NONE")?
		.expect_val("true")?
		.expect_val("NONE")?
		.expect_val("NONE")?
		.expect_val("NONE")?
		.expect_val("123")?
		.expect_val("456")?;
	Ok(())
}
