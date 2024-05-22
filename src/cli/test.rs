use crate::cli::check_upgrade;
use crate::cli::version_client::MapVersionClient;
use crate::err::Error;
use std::collections::BTreeMap;

#[test_log::test(tokio::test)]
pub async fn test_version_upgrade() {
	let mut client = MapVersionClient {
		fetch_mock: BTreeMap::new(),
	};
	client
		.fetch_mock
		.insert("latest".to_string(), || -> Result<String, Error> { Ok("1.0.0".to_string()) });
	check_upgrade(&client, "1.0.0")
		.await
		.expect("Expected the versions to be the same and not require an upgrade");
	check_upgrade(&client, "0.9.0")
		.await
		.expect_err("Expected the versions to be different and require an upgrade");
	check_upgrade(&client, "1.1.0")
		.await
		.expect("Expected the versions to be illogical, and not require and upgrade");
}

mod clap {
	use crate::cli::Cli;
	use clap::Parser;

	#[test_log::test(tokio::test)]
	pub async fn version_upgrade() {
		#[derive(Debug)]
		struct Case {
			expected_on: bool,
			input: &'static [&'static str],
		}
		let cases = [
			Case {
				expected_on: true,
				input: &["surreal", "start", "--help"],
			},
			// Case {
			// 	expected_on: true,
			// 	input: &["--check-version-upgrade"],
			// },
			// Case {
			// 	expected_on: true,
			// 	input: &["--check-version-upgrade", "true"],
			// },
			Case {
				expected_on: true,
				input: &["surreal", "start"],
			},
			// Case {
			// 	expected_on: true,
			// 	input: &["surreal", "start", "--check-version-upgrade"],
			// },
			// Case {
			// 	expected_on: true,
			// 	input: &["surreal", "start", "--check-version-upgrade", "true"],
			// },
			// Case {
			// 	expected_on: true,
			// 	input: &["surreal", "--check-version-upgrade", "start"],
			// },
			Case {
				expected_on: true,
				input: &["surreal", "--check-version-upgrade", "true", "start"],
			},
			Case {
				expected_on: false,
				input: &["surreal", "--check-version-upgrade", "false", "start"],
			},
			Case {
				expected_on: false,
				input: &["surreal", "start", "--check-version-upgrade", "false", "start"],
			},
			Case {
				expected_on: false,
				input: &["surreal", "--check-version-upgrade", "false", "start"],
			},
		];

		for (index, case) in cases.iter().enumerate() {
			let cli = Cli::try_parse_from(case.input);
			assert!(
				cli.is_ok(),
				"There was a failure to parse for {index} - {case:?}:\n{}",
				cli.err().unwrap()
			);
			let cli = cli.unwrap();
			assert_eq!(
				cli.online_version_check, case.expected_on,
				"The expected values were incorrect for {index} - {case:?}"
			);
		}
	}
}
