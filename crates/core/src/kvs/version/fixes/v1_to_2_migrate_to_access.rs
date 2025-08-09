use std::sync::Arc;

use crate::kvs::KVKey;
use crate::{
	expr::{
		AccessType, Ident,
		access_type::{JwtAccessVerify, JwtAccessVerifyKey},
		statements::{DefineAccessStatement, define::DefineScopeStatement},
	},
	kvs::Transaction,
};
use anyhow::Result;

pub async fn v1_to_2_migrate_to_access(tx: Arc<Transaction>) -> Result<()> {
	for ns in tx.all_ns().await?.iter() {
		let ns = ns.name.as_str();
		migrate_ns_tokens(tx.clone(), ns).await?;

		for db in tx.all_db(ns).await?.iter() {
			let db = db.name.as_str();
			migrate_db_tokens(tx.clone(), ns, db).await?;

			let scope_keys = collect_db_scope_keys(tx.clone(), ns, db).await?;
			for key in scope_keys.iter() {
				let ac = migrate_db_scope_key(tx.clone(), ns, db, key.to_owned()).await?;
				migrate_sc_tokens(tx.clone(), ns, db, ac).await?;
			}
		}
	}
	Ok(())
}

async fn migrate_ns_tokens(tx: Arc<Transaction>, ns: &str) -> Result<()> {
	// Find all tokens on the namespace level
	let mut beg = crate::key::namespace::all::new(ns).encode_key()?;
	beg.extend_from_slice(&[b'!', b't', b'k', 0x00]);
	let mut end = crate::key::namespace::all::new(ns).encode_key()?;
	end.extend_from_slice(&[b'!', b't', b'k', 0xff]);

	// queue of tokens to migrate
	let mut queue: Vec<Vec<u8>> = Vec::new();

	// Scan the token definitions
	'scan: loop {
		let mut keys = tx.keys(beg.clone()..end.clone(), 1000, None).await?;
		if keys.is_empty() {
			break 'scan;
		}

		// We suffix the last id with a null byte, to prevent scanning it twice (which would result in an infinite loop)
		beg.clone_from(keys.last().unwrap());
		beg.extend_from_slice(b"\0");

		// Assign to queue
		queue.append(&mut keys);
	}

	// Migrate the tokens to accesses
	for key in queue.iter() {
		// Get the value for the old key. We can unwrap the option, as we know that the key exists in the KV store
		let tk: DefineTokenStatement = revision::from_slice(&tx.get(key, None).await?.unwrap())?;
		// Convert into access
		let ac: DefineAccessStatement = tk.into();

		// Delete the old key
		tx.del(key).await?;

		// Construct the new key
		let key = crate::key::namespace::ac::new(ns, &ac.name.0);
		// Set the fixed key
		tx.set(&key, &ac, None).await?;
	}

	Ok(())
}

async fn migrate_db_tokens(tx: Arc<Transaction>, ns: &str, db: &str) -> Result<()> {
	// Find all tokens on the namespace level
	let mut beg = crate::key::database::all::new(ns, db).encode_key()?;
	beg.extend_from_slice(&[b'!', b't', b'k', 0x00]);
	let mut end = crate::key::database::all::new(ns, db).encode_key()?;
	end.extend_from_slice(&[b'!', b't', b'k', 0xff]);

	// queue of tokens to migrate
	let mut queue: Vec<Vec<u8>> = Vec::new();

	// Scan the token definitions
	'scan: loop {
		let mut keys = tx.keys(beg.clone()..end.clone(), 1000, None).await?;
		if keys.is_empty() {
			break 'scan;
		}

		// We suffix the last id with a null byte, to prevent scanning it twice (which would result in an infinite loop)
		beg.clone_from(keys.last().unwrap());
		beg.extend_from_slice(b"\0");

		// Assign to queue
		queue.append(&mut keys);
	}

	// Migrate the tokens to accesses
	for key in queue.iter() {
		// Get the value for the old key. We can unwrap the option, as we know that the key exists in the KV store
		let tk: DefineTokenStatement = revision::from_slice(&tx.get(key, None).await?.unwrap())?;
		// Convert into access
		let ac: DefineAccessStatement = tk.into();

		// Delete the old key
		tx.del(key).await?;

		// Construct the new key
		let key = crate::key::database::ac::new(ns, db, &ac.name.0);
		// Set the fixed key
		tx.set(&key, &ac, None).await?;
	}

	Ok(())
}

async fn collect_db_scope_keys(tx: Arc<Transaction>, ns: &str, db: &str) -> Result<Vec<Vec<u8>>> {
	// Find all tokens on the namespace level
	let mut beg = crate::key::database::all::new(ns, db).encode_key()?;
	beg.extend_from_slice(&[b'!', b's', b'c', 0x00]);
	let mut end = crate::key::database::all::new(ns, db).encode_key()?;
	end.extend_from_slice(&[b'!', b's', b'c', 0xff]);

	// queue of tokens to migrate
	let mut queue: Vec<Vec<u8>> = Vec::new();

	// Scan the token definitions
	'scan: loop {
		let mut keys = tx.keys(beg.clone()..end.clone(), 1000, None).await?;
		if keys.is_empty() {
			break 'scan;
		}

		// We suffix the last id with a null byte, to prevent scanning it twice (which would result in an infinite loop)
		beg.clone_from(keys.last().unwrap());
		beg.extend_from_slice(b"\0");

		// Assign to queue
		queue.append(&mut keys);
	}

	Ok(queue)
}

async fn migrate_db_scope_key(
	tx: Arc<Transaction>,
	ns: &str,
	db: &str,
	key: Vec<u8>,
) -> Result<DefineAccessStatement> {
	// Get the value for the old key. We can unwrap the option, as we know that the key exists in the KV store
	let sc: DefineScopeStatement = revision::from_slice(&tx.get(&key, None).await?.unwrap())?;
	// Convert into access
	let ac: DefineAccessStatement = sc.into();

	// Delete the old key
	tx.del(&key).await?;

	// Extract the name
	let name = ac.name.clone();
	// Construct the new key
	let key = crate::key::database::ac::new(ns, db, &name);
	// Set the fixed key
	tx.set(&key, &ac, None).await?;

	Ok(ac)
}

async fn migrate_sc_tokens(
	tx: Arc<Transaction>,
	ns: &str,
	db: &str,
	ac: DefineAccessStatement,
) -> Result<()> {
	let name = ac.name.clone();
	// Find all tokens on the namespace level
	// 0xb1 = ±
	// Inserting the string manually does not add a null byte at the end of the string.
	// Hence, in the third `extend_from_slice`, we add the null byte manually, followed by the token key prefix
	let mut beg = crate::key::database::all::new(ns, db).encode_key()?;
	beg.extend_from_slice(&[0xb1]);
	beg.extend_from_slice(name.as_bytes());
	beg.extend_from_slice(&[0x00, b'!', b't', b'k', 0x00]);
	let mut end = crate::key::database::all::new(ns, db).encode_key()?;
	end.extend_from_slice(&[0xb1]);
	end.extend_from_slice(name.as_bytes());
	end.extend_from_slice(&[0x00, b'!', b't', b'k', 0xff]);

	// queue of tokens to migrate
	let mut queue: Vec<Vec<u8>> = Vec::new();

	// Scan the token definitions
	'scan: loop {
		let mut keys = tx.keys(beg.clone()..end.clone(), 1000, None).await?;
		if keys.is_empty() {
			break 'scan;
		}

		// We suffix the last id with a null byte, to prevent scanning it twice (which would result in an infinite loop)
		beg.clone_from(keys.last().unwrap());
		beg.extend_from_slice(b"\0");

		// Assign to queue
		queue.append(&mut keys);
	}

	println!("\n==================");
	println!("NS: `{ns}`, DB: `{db}`, SC: `{}`", ac.name);
	println!(
		"Can not automatically merge scope tokens scope into the new scope-converted access method."
	);
	println!(
		"Logging the merged access definitions individually, with their names joined together like `scope_token`."
	);
	println!(
		"The old tokens will be removed from the datastore, but no fixes will be applied. They need manual resolution."
	);
	println!("==================\n");

	// Log example merged accesses
	for key in queue.iter() {
		// Get the value for the old key. We can unwrap the option, as we know that the key exists in the KV store
		let tk: DefineTokenStatement = revision::from_slice(&tx.get(key, None).await?.unwrap())?;

		// Delete the old key
		tx.del(key).await?;

		// Merge the access and token definitions
		let mut merged = merge_ac_and_tk(ac.clone(), tk.clone());
		merged.name = Ident(format!("{}_{}", ac.name.0, tk.name.0));
		println!("{merged:#}\n");
	}

	println!("==================\n");

	Ok(())
}

fn merge_ac_and_tk(ac: DefineAccessStatement, tk: DefineTokenStatement) -> DefineAccessStatement {
	let mut ac = ac;
	ac.access_type = match ac.access_type {
		AccessType::Record(ak) => {
			let mut ak = ak;
			ak.jwt.verify = JwtAccessVerify::Key(JwtAccessVerifyKey {
				alg: tk.kind,
				key: tk.code,
			});
			AccessType::Record(ak)
		}

		// We can not reach this code, because the code which invokes this
		// method only passes record accesses, which we previously constructed
		// based on old scope definitions.
		_ => unreachable!("Unexpected access kind"),
	};
	ac
}
