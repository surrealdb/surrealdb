use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use surrealdb_strand::Strand;

const SHORT: &str = "hello";
const AT_CAP: &str = "abcdefghijklmnopqrstuvw"; // 23 bytes
const LONG: &str =
	"this string is intentionally much longer than twenty three bytes so it must live on the heap";

fn bench_strand_comparison(c: &mut Criterion) {
	let mut group = c.benchmark_group("strand_comparison");

	// 1. Static strings
	let s1_static = Strand::new_static(SHORT);
	let s2_static = Strand::new_static(SHORT);
	let s3_static = Strand::new_static("world");

	let str1_static = String::from(SHORT);
	let str2_static = String::from(SHORT);
	let str3_static = String::from("world");

	group.bench_function("static_eq_strand", |b| {
		b.iter(|| {
			black_box(black_box(&s1_static) == black_box(&s2_static));
		})
	});
	group.bench_function("static_eq_string", |b| {
		b.iter(|| {
			black_box(black_box(&str1_static) == black_box(&str2_static));
		})
	});
	group.bench_function("static_neq_strand", |b| {
		b.iter(|| {
			black_box(black_box(&s1_static) == black_box(&s3_static));
		})
	});
	group.bench_function("static_neq_string", |b| {
		b.iter(|| {
			black_box(black_box(&str1_static) == black_box(&str3_static));
		})
	});

	// 2. Short strings (inline)
	let s1_short = Strand::from(SHORT);
	let s2_short = Strand::from(SHORT);
	let s3_short = Strand::from("world");

	let str1_short = String::from(SHORT);
	let str2_short = String::from(SHORT);
	let str3_short = String::from("world");

	group.bench_function("short_eq_strand", |b| {
		b.iter(|| {
			black_box(black_box(&s1_short) == black_box(&s2_short));
		})
	});
	group.bench_function("short_eq_string", |b| {
		b.iter(|| {
			black_box(black_box(&str1_short) == black_box(&str2_short));
		})
	});
	group.bench_function("short_neq_strand", |b| {
		b.iter(|| {
			black_box(black_box(&s1_short) == black_box(&s3_short));
		})
	});
	group.bench_function("short_neq_string", |b| {
		b.iter(|| {
			black_box(black_box(&str1_short) == black_box(&str3_short));
		})
	});

	// 3. At capacity strings (inline)
	let s1_cap = Strand::from(AT_CAP);
	let s2_cap = Strand::from(AT_CAP);
	let s3_cap = Strand::from("abcdefghijklmnopqrstuvz");

	let str1_cap = String::from(AT_CAP);
	let str2_cap = String::from(AT_CAP);
	let str3_cap = String::from("abcdefghijklmnopqrstuvz");

	group.bench_function("cap_eq_strand", |b| {
		b.iter(|| {
			black_box(black_box(&s1_cap) == black_box(&s2_cap));
		})
	});
	group.bench_function("cap_eq_string", |b| {
		b.iter(|| {
			black_box(black_box(&str1_cap) == black_box(&str2_cap));
		})
	});
	group.bench_function("cap_neq_strand", |b| {
		b.iter(|| {
			black_box(black_box(&s1_cap) == black_box(&s3_cap));
		})
	});
	group.bench_function("cap_neq_string", |b| {
		b.iter(|| {
			black_box(black_box(&str1_cap) == black_box(&str3_cap));
		})
	});

	// 4. Long strings (boxed)
	let s1_long = Strand::from(LONG);
	let s2_long = Strand::from(LONG);
	let s3_long = Strand::from(
		"this string is intentionally much longer than twenty three bytes so it must live on the heap!",
	);

	let str1_long = String::from(LONG);
	let str2_long = String::from(LONG);
	let str3_long = String::from(
		"this string is intentionally much longer than twenty three bytes so it must live on the heap!",
	);

	group.bench_function("long_eq_strand", |b| {
		b.iter(|| {
			black_box(black_box(&s1_long) == black_box(&s2_long));
		})
	});
	group.bench_function("long_eq_string", |b| {
		b.iter(|| {
			black_box(black_box(&str1_long) == black_box(&str2_long));
		})
	});
	group.bench_function("long_neq_strand", |b| {
		b.iter(|| {
			black_box(black_box(&s1_long) == black_box(&s3_long));
		})
	});
	group.bench_function("long_neq_string", |b| {
		b.iter(|| {
			black_box(black_box(&str1_long) == black_box(&str3_long));
		})
	});

	group.finish();
}

fn bench_strand_ordering(c: &mut Criterion) {
	let mut group = c.benchmark_group("strand_ordering");

	// 1. Static strings
	let s1_static = Strand::new_static(SHORT);
	let s2_static = Strand::new_static("world");

	let str1_static = String::from(SHORT);
	let str2_static = String::from("world");

	group.bench_function("static_cmp_strand", |b| {
		b.iter(|| {
			black_box(black_box(&s1_static).cmp(black_box(&s2_static)));
		})
	});
	group.bench_function("static_cmp_string", |b| {
		b.iter(|| {
			black_box(black_box(&str1_static).cmp(black_box(&str2_static)));
		})
	});

	// 2. Short strings (inline)
	let s1_short = Strand::from(SHORT);
	let s2_short = Strand::from("world");

	let str1_short = String::from(SHORT);
	let str2_short = String::from("world");

	group.bench_function("short_cmp_strand", |b| {
		b.iter(|| {
			black_box(black_box(&s1_short).cmp(black_box(&s2_short)));
		})
	});
	group.bench_function("short_cmp_string", |b| {
		b.iter(|| {
			black_box(black_box(&str1_short).cmp(black_box(&str2_short)));
		})
	});

	// 3. At capacity strings (inline)
	let s1_cap = Strand::from(AT_CAP);
	let s2_cap = Strand::from("abcdefghijklmnopqrstuvz");

	let str1_cap = String::from(AT_CAP);
	let str2_cap = String::from("abcdefghijklmnopqrstuvz");

	group.bench_function("cap_cmp_strand", |b| {
		b.iter(|| {
			black_box(black_box(&s1_cap).cmp(black_box(&s2_cap)));
		})
	});
	group.bench_function("cap_cmp_string", |b| {
		b.iter(|| {
			black_box(black_box(&str1_cap).cmp(black_box(&str2_cap)));
		})
	});

	// 4. Long strings (boxed)
	let s1_long = Strand::from(LONG);
	let s2_long = Strand::from(
		"this string is intentionally much longer than twenty three bytes so it must live on the heap!",
	);

	let str1_long = String::from(LONG);
	let str2_long = String::from(
		"this string is intentionally much longer than twenty three bytes so it must live on the heap!",
	);

	group.bench_function("long_cmp_strand", |b| {
		b.iter(|| {
			black_box(black_box(&s1_long).cmp(black_box(&s2_long)));
		})
	});
	group.bench_function("long_cmp_string", |b| {
		b.iter(|| {
			black_box(black_box(&str1_long).cmp(black_box(&str2_long)));
		})
	});

	group.finish();
}

fn bench_strand_cloning(c: &mut Criterion) {
	let mut group = c.benchmark_group("strand_cloning");

	// 1. Static strings
	let s_static = Strand::new_static(SHORT);
	let str_static = String::from(SHORT);

	group.bench_function("static_clone_strand", |b| {
		b.iter(|| {
			black_box(black_box(&s_static).clone());
		})
	});
	group.bench_function("static_clone_string", |b| {
		b.iter(|| {
			black_box(black_box(&str_static).clone());
		})
	});

	// 2. Short strings (inline)
	let s_short = Strand::from(SHORT);
	let str_short = String::from(SHORT);

	group.bench_function("short_clone_strand", |b| {
		b.iter(|| {
			black_box(black_box(&s_short).clone());
		})
	});
	group.bench_function("short_clone_string", |b| {
		b.iter(|| {
			black_box(black_box(&str_short).clone());
		})
	});

	// 3. At capacity strings (inline)
	let s_cap = Strand::from(AT_CAP);
	let str_cap = String::from(AT_CAP);

	group.bench_function("cap_clone_strand", |b| {
		b.iter(|| {
			black_box(black_box(&s_cap).clone());
		})
	});
	group.bench_function("cap_clone_string", |b| {
		b.iter(|| {
			black_box(black_box(&str_cap).clone());
		})
	});

	// 4. Long strings (boxed)
	let s_long = Strand::from(LONG);
	let str_long = String::from(LONG);

	group.bench_function("long_clone_strand", |b| {
		b.iter(|| {
			black_box(black_box(&s_long).clone());
		})
	});
	group.bench_function("long_clone_string", |b| {
		b.iter(|| {
			black_box(black_box(&str_long).clone());
		})
	});

	group.finish();
}

criterion_group!(benches, bench_strand_comparison, bench_strand_ordering, bench_strand_cloning);
criterion_main!(benches);
