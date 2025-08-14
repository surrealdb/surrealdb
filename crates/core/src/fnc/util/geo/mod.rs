use geo::Point;

use crate::val::{Geometry, Strand};

static BASE32: &[char] = &[
	'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k',
	'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
];

pub fn encode(v: Point<f64>, l: usize) -> Strand {
	let mut max_lat = 90f64;
	let mut min_lat = -90f64;
	let mut max_lon = 180f64;
	let mut min_lon = -180f64;

	let mut bits: i8 = 0;
	let mut hash: usize = 0;
	let mut out = String::with_capacity(l);

	while out.len() < l {
		for _ in 0..5 {
			if bits % 2 == 0 {
				let mid = (max_lon + min_lon) / 2f64;
				if v.x() > mid {
					hash = (hash << 1) + 1usize;
					min_lon = mid;
				} else {
					hash <<= 1;
					max_lon = mid;
				}
			} else {
				let mid = (max_lat + min_lat) / 2f64;
				if v.y() > mid {
					hash = (hash << 1) + 1usize;
					min_lat = mid;
				} else {
					hash <<= 1;
					max_lat = mid;
				}
			}
			bits += 1;
		}
		out.push(BASE32[hash]);
		hash = 0;
	}

	Strand::from(out)
}

pub fn decode(v: Strand) -> Geometry {
	let mut max_lat = 90f64;
	let mut min_lat = -90f64;
	let mut max_lon = 180f64;
	let mut min_lon = -180f64;

	let mut mid: f64;
	let mut long: bool = true;

	for c in v.as_str().chars() {
		let ord = c as usize;

		let val = if (48..=57).contains(&ord) {
			ord - 48
		} else if (98..=104).contains(&ord) {
			ord - 88
		} else if (106..=107).contains(&ord) {
			ord - 89
		} else if (109..=110).contains(&ord) {
			ord - 90
		} else if (112..=122).contains(&ord) {
			ord - 91
		} else {
			ord
		};

		for i in 0..5 {
			let bit = (val >> (4 - i)) & 1usize;
			if long {
				mid = (max_lon + min_lon) / 2f64;
				if bit == 1 {
					min_lon = mid;
				} else {
					max_lon = mid;
				}
			} else {
				mid = (max_lat + min_lat) / 2f64;
				if bit == 1 {
					min_lat = mid;
				} else {
					max_lat = mid;
				}
			}
			long = !long;
		}
	}

	let x = (min_lon + max_lon) / 2f64;
	let y = (min_lat + max_lat) / 2f64;

	(x, y).into()
}
