//! The greeting a front end sends before handing a connection over (`greeting_packet`) is pinned
//! byte for byte: Earth's SQL front builds the same packet without linking this crate.

use surrealdb_mywire::greeting_packet;

const GREETING: &str = "540000000a382e302e33352d5375727265616c4442000403020165617274682d7371000faa2d02002f0115000000000000000000006c2d6772656574696e672d310063616368696e675f736861325f70617373776f726400";

#[test]
fn the_greeting_for_a_connection_id_is_pinned() {
	let hex: String = greeting_packet(0x0102_0304).iter().map(|b| format!("{b:02x}")).collect();
	assert_eq!(hex, GREETING);
}
