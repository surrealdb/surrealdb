#[cfg(test)]
mod tests {
	use surrealdb::Surreal;
	use surrealdb::engine::any;
	use surrealdb::types::{RecordId, SurrealValue};

	#[derive(Debug, SurrealValue)]
	struct TestRecord {
		id: RecordId,
		predicate: Option<String>,
	}

	#[derive(Debug, SurrealValue)]
	struct CountResult {
		count: u64,
	}

	#[tokio::test]
	async fn test_create_visibility_bug() -> Result<(), Box<dyn std::error::Error>> {
		println!(
			"Reproducing SurrealDB issue #6242: Newly created rows increase count() but remain invisible"
		);
		println!(
			"========================================================================================="
		);

		// Test with embedded mem:// engine
		let client: Surreal<_> = any::connect("mem://").await?;
		client.use_ns("vis_repro_mem").use_db("test").await?;

		// Define the table
		client.query("DEFINE TABLE relationship_node_plain;").await?;
		println!("✓ Table defined");

		// Test 1: Basic CREATE with RETURN id
		println!("\n1. Testing CREATE with RETURN id:");
		let mut q =
			client.query("CREATE relationship_node_plain SET predicate='p1' RETURN id;").await?;
		let create_rows: Vec<serde_json::Value> = q.take(0).unwrap_or_default();
		println!("   CREATE result: {:?}", create_rows);

		// Test 2: Check count
		println!("\n2. Testing count():");
		let mut cnt = client.query("SELECT count() FROM relationship_node_plain;").await?;
		let cnt_rows: Vec<serde_json::Value> = cnt.take(0).unwrap_or_default();
		println!("   Count result: {:?}", cnt_rows);

		// Test 3: Check SELECT
		println!("\n3. Testing SELECT:");
		let mut sel = client.query("SELECT id, predicate FROM relationship_node_plain;").await?;
		let sel_rows: Vec<serde_json::Value> = sel.take(0).unwrap_or_default();
		println!("   SELECT result: {:?}", sel_rows);

		// Test 4: Try with RETURN *
		println!("\n4. Testing CREATE with RETURN *:");
		let mut q2 =
			client.query("CREATE relationship_node_plain SET predicate='p2' RETURN *;").await?;
		let create_rows2: Vec<serde_json::Value> = q2.take(0).unwrap_or_default();
		println!("   CREATE RETURN * result: {:?}", create_rows2);

		// Test 5: Try with explicit ID
		println!("\n5. Testing CREATE with explicit ID:");
		let mut q3 = client
			.query("CREATE relationship_node_plain:123 SET predicate='p3' RETURN id;")
			.await?;
		let create_rows3: Vec<serde_json::Value> = q3.take(0).unwrap_or_default();
		println!("   CREATE with explicit ID result: {:?}", create_rows3);

		// Test 6: Check if explicit record exists
		println!("\n6. Testing SELECT with explicit ID:");
		let mut sel2 = client.query("SELECT * FROM relationship_node_plain:123;").await?;
		let sel_rows2: Vec<serde_json::Value> = sel2.take(0).unwrap_or_default();
		println!("   SELECT explicit ID result: {:?}", sel_rows2);

		// Test 7: Final count check
		println!("\n7. Final count check:");
		let mut cnt2 = client.query("SELECT count() FROM relationship_node_plain;").await?;
		let cnt_rows2: Vec<serde_json::Value> = cnt2.take(0).unwrap_or_default();
		println!("   Final count result: {:?}", cnt_rows2);

		// Test 8: Try with CONTENT
		println!("\n8. Testing CREATE with CONTENT:");
		let mut q4 = client
			.query(
				"CREATE relationship_node_plain CONTENT { predicate: 'content1', extra: 42 } RETURN *;",
			)
			.await?;
		let create_rows4: Vec<serde_json::Value> = q4.take(0).unwrap_or_default();
		println!("   CREATE CONTENT result: {:?}", create_rows4);

		println!(
			"\n========================================================================================="
		);
		println!("Bug analysis:");
		println!("- If CREATE RETURN results are empty but count() > 0, the bug is reproduced");
		println!(
			"- Expected: CREATE should return the created records and SELECT should show them"
		);

		// Assertions to verify the bug
		let count_val =
			cnt_rows2.first().and_then(|v| v.get("count")).and_then(|v| v.as_u64()).unwrap_or(0);
		println!("\nBUG CHECK:");
		println!("- Count reported: {}", count_val);
		println!("- CREATE results empty: {}", create_rows.is_empty());
		println!("- SELECT results empty: {}", sel_rows.is_empty());

		if count_val > 0 && create_rows.is_empty() && sel_rows.is_empty() {
			println!("🐛 BUG REPRODUCED: Count shows {} but no visible records!", count_val);
		} else {
			println!("✅ No bug detected or bug has been fixed");
		}

		Ok(())
	}

	#[tokio::test]
	async fn test_manual_sequence() -> Result<(), Box<dyn std::error::Error>> {
		println!("Testing the exact sequence from the user");

		let client: Surreal<_> = any::connect("mem://").await?;
		client.use_ns("manual_test").use_db("test").await?;

		// Run the exact sequence the user mentioned
		println!("1. DEFINE TABLE relationship_node_plain;");
		client.query("DEFINE TABLE relationship_node_plain;").await?;

		println!("2. CREATE relationship_node_plain SET predicate='p1' RETURN id;");
		let mut create_result =
			client.query("CREATE relationship_node_plain SET predicate='p1' RETURN id;").await?;

		// Try to deserialize the CREATE result into proper types
		let create_rows = match create_result.take::<Vec<TestRecord>>(0) {
			Ok(rows) => {
				println!("   CREATE result: {:?}", rows);
				rows
			}
			Err(e) => {
				println!("   CREATE ERROR: {:?}", e);
				return Err(e.into());
			}
		};

		println!("3. SELECT id, predicate FROM relationship_node_plain;");
		let mut select_result =
			client.query("SELECT id, predicate FROM relationship_node_plain;").await?;
		let select_rows = match select_result.take::<Vec<TestRecord>>(0) {
			Ok(rows) => {
				println!("   SELECT result: {:?}", rows);
				rows
			}
			Err(e) => {
				println!("   SELECT ERROR: {:?}", e);
				return Err(e.into());
			}
		};

		println!("4. SELECT count() FROM relationship_node_plain;");
		let mut count_result = client.query("SELECT count() FROM relationship_node_plain;").await?;
		let count_rows = match count_result.take::<Vec<CountResult>>(0) {
			Ok(rows) => {
				println!("   COUNT result: {:?}", rows);
				rows
			}
			Err(e) => {
				println!("   COUNT ERROR: {:?}", e);
				return Err(e.into());
			}
		};

		// Analysis
		let count_val = count_rows.first().map(|c| c.count).unwrap_or(0);
		println!("\nAnalysis:");
		println!("- CREATE returned {} rows", create_rows.len());
		println!("- SELECT returned {} rows", select_rows.len());
		println!("- COUNT shows {} records", count_val);

		if count_val > 0 && select_rows.is_empty() {
			println!("🐛 BUG: Count shows records but SELECT returns empty!");
		} else if count_val > 0 && !select_rows.is_empty() {
			println!("✅ WORKING: Records are properly visible");
		} else {
			println!("❓ UNCLEAR: No records created or other issue");
		}

		Ok(())
	}
}
