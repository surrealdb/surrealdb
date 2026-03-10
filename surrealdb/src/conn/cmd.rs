use std::borrow::Cow;
use std::path::PathBuf;

use async_channel::Sender;
use surrealdb_core::iam::token::Token;
use surrealdb_core::kvs::export::Config as DbExportConfig;
use uuid::Uuid;

use super::MlExportConfig;
use crate::types::{Array, Notification, Object, Value, Variables};

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) enum Command {
	Use {
		namespace: Option<String>,
		database: Option<String>,
	},
	Signup {
		credentials: Object,
	},
	Signin {
		credentials: Object,
	},
	Authenticate {
		token: Token,
	},
	Refresh {
		token: Token,
	},
	Invalidate,
	Begin,
	Rollback {
		txn: Uuid,
	},
	Commit {
		txn: Uuid,
	},
	Revoke {
		token: Token,
	},
	Query {
		txn: Option<Uuid>,
		query: Cow<'static, str>,
		variables: Variables,
	},
	ExportFile {
		path: PathBuf,
		config: Option<DbExportConfig>,
	},
	ExportMl {
		path: PathBuf,
		config: MlExportConfig,
	},
	ExportBytes {
		bytes: Sender<crate::Result<Vec<u8>>>,
		config: Option<DbExportConfig>,
	},
	ExportBytesMl {
		bytes: Sender<crate::Result<Vec<u8>>>,
		config: MlExportConfig,
	},
	ImportFile {
		path: PathBuf,
	},
	ImportMl {
		path: PathBuf,
	},
	Health,
	Version,
	Set {
		key: String,
		value: Value,
	},
	Unset {
		key: String,
	},
	SubscribeLive {
		uuid: Uuid,
		notification_sender: Sender<crate::Result<Notification>>,
	},
	Kill {
		uuid: Uuid,
	},
	Attach {
		session_id: Uuid,
	},
	Detach {
		session_id: Uuid,
	},
	Run {
		name: String,
		version: Option<String>,
		args: Array,
	},
}
