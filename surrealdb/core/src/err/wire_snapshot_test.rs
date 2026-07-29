//! Wire-format snapshot for every [`Error`] variant.
//!
//! [`into_types_error`] is the only place an internal error becomes the public
//! `surrealdb_types::Error`, and its shape is a compatibility surface: clients
//! branch on `code` and `kind`, not on the message text. The language-test
//! suite pins messages and nothing else, so this snapshot pins the rest.
//!
//! One sample of every variant is mapped and recorded in its serialised form,
//! which carries `code`, `kind`, `details`, `message` and `cause` together. A
//! diff here means the wire contract moved. That is sometimes intended, but it
//! is never incidental: regenerate deliberately (the failure message says how)
//! and review the diff.

use common::LeafError;
use surrealdb_types::{SurrealValue, ToSql};

use super::{EngineError, Error};
use crate::api::err::ApiError;
use crate::buc::Error as BucError;
use crate::catalog::Error as CatalogError;
use crate::dbs::SortError;
use crate::dbs::capabilities::Error as CapabilitiesError;
use crate::doc::Error as DocError;
use crate::exec::Error as ExecError;
use crate::expr::operation::PatchError;
use crate::expr::{Bytesize, Error as ExprError, Expr, Idiom};
use crate::iam::{Error as AuthError, PolicyError};
use crate::idx::Error as IdxError;
use crate::key::Error as KeyError;
use crate::kvs::{DatastoreError, Error as KvsError};
use crate::syn::ParseError;
use crate::val::{CastError, CoerceError, Duration, RecordId, Value};

fn sample_coerce_error() -> CoerceError {
	CoerceError::InvalidKind {
		from: Value::None,
		into: "sample".to_string(),
	}
}

fn sample_cast_error() -> CastError {
	CastError::InvalidKind {
		from: Value::None,
		into: "sample".to_string(),
	}
}

fn sample_rendered_error() -> crate::syn::error::RenderedError {
	crate::syn::error::RenderedError {
		errors: vec!["sample".to_string()],
		snippets: Vec::new(),
	}
}

/// One instance of every `Error` variant, in declaration order.
///
/// Payloads are deliberately uniform placeholders: the snapshot records how a
/// variant is *classified*, not how a real occurrence reads.
fn every_variant() -> Vec<(&'static str, Error)> {
	vec![
		// Sampled with a classified variant on purpose: the row must come out
		// identical to the bare `QueryCancelled` one below, which is what says
		// the transparent wrapper neither reclassifies nor adds a cause.
		("Engine", Error::Engine(EngineError::QueryCancelled)),
		// Sampled the same way, against the bare `NsEmpty` row in the exec
		// block below.
		("Exec", Error::Exec(ExecError::NsEmpty)),
		("Kvs", Error::Kvs(KvsError::Internal("sample".to_string()))),
		("IamError", Error::IamError(PolicyError::InvalidRole("sample".to_string()))),
		// Sampled the same way again, against the bare `BodyDecodeFailure` row
		// in the api block below.
		("InvalidPath", Error::InvalidPath("sample".to_string())),
		("ApiError", Error::ApiError(ApiError::BodyDecodeFailure)),
		("InvalidUrl", Error::InvalidUrl("sample".to_string())),
		("Http", Error::Http("sample".to_string())),
	]
}

/// One instance of every [`CapabilitiesError`] variant.
fn every_capabilities_variant() -> Vec<(&'static str, CapabilitiesError)> {
	vec![
		("HttpDisabled", CapabilitiesError::HttpDisabled),
		("ScriptingNotAllowed", CapabilitiesError::ScriptingNotAllowed),
		("FunctionNotAllowed", CapabilitiesError::FunctionNotAllowed("sample".to_string())),
		("NetTargetNotAllowed", CapabilitiesError::NetTargetNotAllowed("sample".to_string())),
	]
}

/// One instance of every [`SortError`] variant.
fn every_sort_variant() -> Vec<(&'static str, SortError)> {
	vec![
		("Io", SortError::Io(std::io::Error::other("sample"))),
		("Revision", SortError::Revision(revision::Error::Serialize("sample".to_string()))),
		("OrderingError", SortError::OrderingError("sample".to_string())),
	]
}

/// One instance of every [`ParseError`] variant.
fn every_parse_variant() -> Vec<(&'static str, ParseError)> {
	vec![
		("QueryTooLarge", ParseError::QueryTooLarge),
		("InvalidQuery", ParseError::InvalidQuery(sample_rendered_error())),
	]
}

/// One instance of every [`ApiError`] variant.
fn every_api_variant() -> Vec<(&'static str, ApiError)> {
	vec![
		("InvalidRequestBody", ApiError::InvalidRequestBody),
		("RequestBodyTooLarge", ApiError::RequestBodyTooLarge(Bytesize::kb(1))),
		("BodyDecodeFailure", ApiError::BodyDecodeFailure),
		("BodyEncodeFailure", ApiError::BodyEncodeFailure),
		("InvalidApiResponse", ApiError::InvalidApiResponse("sample".to_string())),
		("InvalidFormat", ApiError::InvalidFormat),
		("MissingFormat", ApiError::MissingFormat),
		("ApiUnreachable", ApiError::Unreachable("sample".to_string())),
		("InvalidStatusCode", ApiError::InvalidStatusCode(1)),
		("InvalidHeaderName", ApiError::InvalidHeaderName("sample".to_string())),
		(
			"InvalidHeaderValue",
			ApiError::InvalidHeaderValue {
				name: "sample".to_string(),
				value: "sample".to_string(),
			},
		),
		("HeaderInjectionAttempt", ApiError::HeaderInjectionAttempt("sample".to_string())),
		("MissingContentType", ApiError::MissingContentType),
		("UnsupportedContentType", ApiError::UnsupportedContentType("sample".to_string())),
		("InvalidContentType", ApiError::InvalidContentType("sample".to_string())),
		("NoOutputStrategy", ApiError::NoOutputStrategy),
		(
			"InvalidRequestBodyType",
			ApiError::InvalidRequestBodyType {
				expected: "sample".to_string(),
				actual: "sample".to_string(),
			},
		),
		(
			"MiddlewareRequestParseFailure",
			ApiError::MiddlewareRequestParseFailure {
				middleware: "sample".to_string(),
			},
		),
		(
			"MiddlewareFunctionNotFound",
			ApiError::MiddlewareFunctionNotFound {
				function: "sample".to_string(),
			},
		),
		("FinalActionRequestParseFailure", ApiError::FinalActionRequestParseFailure),
		("RequestBodyNotBinary", ApiError::RequestBodyNotBinary),
		("PermissionDenied", ApiError::PermissionDenied),
		("NotFound", ApiError::NotFound),
	]
}

/// One instance of every [`EngineError`] variant.
///
/// These are the failures any layer can raise, so they are snapshotted
/// alongside core's own to keep the coverage assertion honest.
fn every_engine_variant() -> Vec<(&'static str, EngineError)> {
	vec![
		("Unreachable", EngineError::Unreachable("sample".to_string())),
		("QueryTimedout", EngineError::QueryTimedout(std::time::Duration::from_secs(1))),
		("QueryCancelled", EngineError::QueryCancelled),
		("Internal", EngineError::Internal("sample".to_string())),
	]
}

/// One instance of every [`crate::key::Error`] variant.
fn every_key_variant() -> Vec<(&'static str, KeyError)> {
	vec![("Unencodable", KeyError::Unencodable), ("Corrupted", KeyError::Corrupted("sample"))]
}

/// One instance of every [`crate::expr::Error`] variant.
fn every_expr_variant() -> Vec<(&'static str, ExprError)> {
	vec![
		(
			"InvalidContent",
			ExprError::InvalidContent {
				value: Value::None,
			},
		),
		(
			"InvalidMerge",
			ExprError::InvalidMerge {
				value: Value::None,
			},
		),
		(
			"InvalidPatch",
			ExprError::InvalidPatch(PatchError {
				message: "sample".to_string(),
			}),
		),
		(
			"PatchTest",
			ExprError::PatchTest {
				expected: "sample".to_string(),
				got: "sample".to_string(),
			},
		),
		(
			"InvalidFunctionArguments",
			ExprError::InvalidFunctionArguments {
				name: "sample".to_string(),
				message: "sample".to_string(),
			},
		),
		("InvalidRegex", ExprError::InvalidRegex("sample".to_string())),
		(
			"IdInvalid",
			ExprError::IdInvalid {
				value: "sample".to_string(),
			},
		),
		("Coerce", ExprError::Coerce(sample_coerce_error())),
		("Cast", ExprError::Cast(sample_cast_error())),
		("TryAdd", ExprError::TryAdd("sample".to_string(), "sample".to_string())),
		("TrySub", ExprError::TrySub("sample".to_string(), "sample".to_string())),
		("TryMul", ExprError::TryMul("sample".to_string(), "sample".to_string())),
		("TryDiv", ExprError::TryDiv("sample".to_string(), "sample".to_string())),
		("TryRem", ExprError::TryRem("sample".to_string(), "sample".to_string())),
		("TryPow", ExprError::TryPow("sample".to_string(), "sample".to_string())),
		("TryNeg", ExprError::TryNeg("sample".to_string())),
		("TryExtend", ExprError::TryExtend("sample".to_string())),
		("TryFrom", ExprError::TryFrom("sample".to_string(), "sample")),
		("TimestampOverflow", ExprError::TimestampOverflow("sample".to_string())),
		("Serialization", ExprError::Serialization("sample".to_string())),
		(
			"TbInvalid",
			ExprError::TbInvalid {
				value: "sample".to_string(),
			},
		),
		(
			"UnsupportedDestructure",
			ExprError::UnsupportedDestructure {
				variant: "sample".to_string(),
			},
		),
		("ArithmeticOverflow", ExprError::ArithmeticOverflow("sample".to_string())),
		("ArithmeticNegativeOverflow", ExprError::ArithmeticNegativeOverflow("sample".to_string())),
		("InvalidBytesize", ExprError::InvalidBytesize),
	]
}

/// One instance of every [`crate::idx::Error`] variant.
fn every_idx_variant() -> Vec<(&'static str, IdxError)> {
	vec![
		(
			"InvalidVectorDimension",
			IdxError::InvalidVectorDimension {
				current: 1,
				expected: 1,
			},
		),
		("InvalidVectorValue", IdxError::InvalidVectorValue("sample".to_string())),
		(
			"IndexExists",
			IdxError::IndexExists {
				record: RecordId::new("t".into(), "1".to_string()),
				index: "sample".to_string(),
				value: "sample".to_string(),
			},
		),
		(
			"NoIndexFoundForMatch",
			IdxError::NoIndexFoundForMatch {
				exp: "sample".to_string(),
			},
		),
		("AnalyzerError", IdxError::AnalyzerError("sample".to_string())),
		("HighlightError", IdxError::HighlightError("sample".to_string())),
		("FstError", IdxError::FstError(fst::Error::Io(std::io::Error::other("sample")))),
		(
			"DuplicatedMatchRef",
			IdxError::DuplicatedMatchRef {
				mr: 1,
			},
		),
	]
}

/// One instance of every [`crate::kvs::DatastoreError`] variant.
fn every_datastore_variant() -> Vec<(&'static str, DatastoreError)> {
	vec![
		("ExpiredSession", DatastoreError::ExpiredSession),
		("RealtimeDisabled", DatastoreError::RealtimeDisabled),
		("InvalidTimeout", DatastoreError::InvalidTimeout(1)),
		(
			"TransactionTimedout",
			DatastoreError::TransactionTimedout(Duration(std::time::Duration::from_secs(1))),
		),
		("QueryBeyondMemoryThreshold", DatastoreError::QueryBeyondMemoryThreshold),
		(
			"TransactionWriteKeysExceeded",
			DatastoreError::TransactionWriteKeysExceeded {
				limit: 1,
			},
		),
		(
			"QueryNotExecuted",
			DatastoreError::QueryNotExecuted {
				message: "sample".to_string(),
			},
		),
		("CorruptedIndex", DatastoreError::CorruptedIndex("sample")),
		(
			"IndexAlreadyBuilding",
			DatastoreError::IndexAlreadyBuilding {
				name: "sample".to_string(),
			},
		),
		(
			"IndexingBuildingCancelled",
			DatastoreError::IndexingBuildingCancelled {
				reason: "sample".to_string(),
			},
		),
		("InvalidStorageVersion", DatastoreError::InvalidStorageVersion),
		(
			"OutdatedStorageVersion",
			DatastoreError::OutdatedStorageVersion {
				expected: 1,
				actual: 1,
			},
		),
	]
}

/// One instance of every [`crate::buc::Error`] variant.
fn every_buc_variant() -> Vec<(&'static str, BucError)> {
	vec![
		(
			"BucketPermissions",
			BucError::BucketPermissions {
				name: "sample".to_string(),
				op: crate::buc::BucketOperation::Get,
			},
		),
		(
			"ObsError",
			BucError::ObsError(object_store::Error::NotSupported {
				source: "sample".into(),
			}),
		),
		("FileAccessDenied", BucError::FileAccessDenied("sample".to_string())),
		("NoGlobalBucket", BucError::NoGlobalBucket),
		("BucketUnavailable", BucError::BucketUnavailable("sample".to_string())),
		("GlobalBucketEnforced", BucError::GlobalBucketEnforced),
		("InvalidBucketUrl", BucError::InvalidBucketUrl("sample".to_string())),
		("UnsupportedBackend", BucError::UnsupportedBackend),
		("ReadonlyBucket", BucError::ReadonlyBucket("sample".to_string())),
		(
			"ObjectStoreFailure",
			BucError::ObjectStoreFailure("sample".to_string(), "sample".to_string()),
		),
	]
}

/// One instance of every [`crate::doc::Error`] variant.
fn every_doc_variant() -> Vec<(&'static str, DocError)> {
	vec![
		(
			"RecordExists",
			DocError::RecordExists {
				record: RecordId::new("t".into(), "1".to_string()),
			},
		),
		(
			"IdNotFound",
			DocError::IdNotFound {
				rid: "sample".to_string(),
			},
		),
		(
			"IdMismatch",
			DocError::IdMismatch {
				value: "sample".to_string(),
			},
		),
		(
			"IdFieldGenerateUnsupported",
			DocError::IdFieldGenerateUnsupported {
				table: "sample".to_string(),
				kind: "sample".to_string(),
			},
		),
		(
			"InOverride",
			DocError::InOverride {
				value: "sample".to_string(),
			},
		),
		(
			"OutOverride",
			DocError::OutOverride {
				value: "sample".to_string(),
			},
		),
		(
			"TableCheck",
			DocError::TableCheck {
				record: "sample".to_string(),
				relation: true,
				target_type: "sample".to_string(),
			},
		),
		(
			"TableIsView",
			DocError::TableIsView {
				table: "sample".to_string(),
			},
		),
		(
			"FieldValue",
			DocError::FieldValue {
				record: "sample".to_string(),
				value: "sample".to_string(),
				field: Idiom::field("f".to_string()),
				check: "sample".to_string(),
			},
		),
		(
			"FieldReadonly",
			DocError::FieldReadonly {
				record: "sample".to_string(),
				field: Idiom::field("f".to_string()),
			},
		),
		(
			"FieldUndefined",
			DocError::FieldUndefined {
				table: "sample".to_string(),
				field: Idiom::field("f".to_string()),
			},
		),
		(
			"FieldCoerce",
			DocError::FieldCoerce {
				record: "sample".to_string(),
				field_name: "sample".to_string(),
				error: Box::new(sample_coerce_error()),
			},
		),
		(
			"DeleteRejectedByReference",
			DocError::DeleteRejectedByReference("sample".to_string(), "sample".to_string()),
		),
		(
			"RefsUpdateFailure",
			DocError::RefsUpdateFailure("sample".to_string(), "sample".to_string()),
		),
		(
			"EvNamespaceMismatch",
			DocError::EvNamespaceMismatch("sample".to_string(), "sample".to_string()),
		),
		(
			"EvDatabaseMismatch",
			DocError::EvDatabaseMismatch("sample".to_string(), "sample".to_string()),
		),
		("EvReachMaxDepth", DocError::EvReachMaxDepth("sample".to_string(), 1)),
	]
}

/// One instance of every [`crate::iam::Error`] variant.
fn every_auth_variant() -> Vec<(&'static str, AuthError)> {
	vec![
		("TokenMakingFailed", AuthError::TokenMakingFailed),
		("NoRecordFound", AuthError::NoRecordFound),
		("MissingUserOrPass", AuthError::MissingUserOrPass),
		("NoSigninTarget", AuthError::NoSigninTarget),
		("InvalidPass", AuthError::InvalidPass),
		("InvalidAuth", AuthError::InvalidAuth),
		("UnexpectedAuth", AuthError::UnexpectedAuth),
		("InvalidSignup", AuthError::InvalidSignup),
		("ExpiredToken", AuthError::ExpiredToken),
		("AccessMethodMismatch", AuthError::AccessMethodMismatch),
		("AccessNotFound", AuthError::AccessNotFound),
		("AccessInvalidDuration", AuthError::AccessInvalidDuration),
		("AccessInvalidExpiration", AuthError::AccessInvalidExpiration),
		("AccessRecordSignupQueryFailed", AuthError::AccessRecordSignupQueryFailed),
		("AccessRecordSigninQueryFailed", AuthError::AccessRecordSigninQueryFailed),
		("AccessRecordNoSignup", AuthError::AccessRecordNoSignup),
		("AccessRecordNoSignin", AuthError::AccessRecordNoSignin),
		("AccessBearerMissingKey", AuthError::AccessBearerMissingKey),
		("AccessGrantBearerInvalid", AuthError::AccessGrantBearerInvalid),
	]
}

/// One instance of every [`crate::catalog::Error`] variant.
fn every_catalog_variant() -> Vec<(&'static str, CatalogError)> {
	vec![
		(
			"NsNotFound",
			CatalogError::NsNotFound {
				name: "sample".to_string(),
			},
		),
		(
			"DbNotFound",
			CatalogError::DbNotFound {
				name: "sample".to_string(),
			},
		),
		(
			"EvNotFound",
			CatalogError::EvNotFound {
				name: "sample".to_string(),
			},
		),
		(
			"FcNotFound",
			CatalogError::FcNotFound {
				name: "sample".to_string(),
			},
		),
		(
			"MdNotFound",
			CatalogError::MdNotFound {
				name: "sample".to_string(),
			},
		),
		(
			"FdNotFound",
			CatalogError::FdNotFound {
				name: "sample".to_string(),
			},
		),
		(
			"MlNotFound",
			CatalogError::MlNotFound {
				name: "sample".to_string(),
			},
		),
		(
			"NdNotFound",
			CatalogError::NdNotFound {
				uuid: "sample".to_string(),
			},
		),
		(
			"PaNotFound",
			CatalogError::PaNotFound {
				name: "sample".to_string(),
			},
		),
		(
			"SeqNotFound",
			CatalogError::SeqNotFound {
				name: "sample".to_string(),
			},
		),
		(
			"CgNotFound",
			CatalogError::CgNotFound {
				name: "sample".to_string(),
			},
		),
		(
			"TbNotFound",
			CatalogError::TbNotFound {
				name: "t".into(),
			},
		),
		(
			"ApNotFound",
			CatalogError::ApNotFound {
				value: "sample".to_string(),
			},
		),
		(
			"AzNotFound",
			CatalogError::AzNotFound {
				name: "sample".to_string(),
			},
		),
		(
			"BuNotFound",
			CatalogError::BuNotFound {
				name: "sample".to_string(),
			},
		),
		(
			"IxNotFound",
			CatalogError::IxNotFound {
				name: "sample".to_string(),
			},
		),
		(
			"UserRootNotFound",
			CatalogError::UserRootNotFound {
				name: "sample".to_string(),
			},
		),
		(
			"UserNsNotFound",
			CatalogError::UserNsNotFound {
				name: "sample".to_string(),
				ns: "sample".to_string(),
			},
		),
		(
			"UserDbNotFound",
			CatalogError::UserDbNotFound {
				name: "sample".to_string(),
				ns: "sample".to_string(),
				db: "sample".to_string(),
			},
		),
		(
			"AccessRootNotFound",
			CatalogError::AccessRootNotFound {
				ac: "sample".to_string(),
			},
		),
		(
			"AccessGrantRootNotFound",
			CatalogError::AccessGrantRootNotFound {
				ac: "sample".to_string(),
				gr: "sample".to_string(),
			},
		),
		(
			"AccessNsNotFound",
			CatalogError::AccessNsNotFound {
				ac: "sample".to_string(),
				ns: "sample".to_string(),
			},
		),
		(
			"AccessGrantNsNotFound",
			CatalogError::AccessGrantNsNotFound {
				ac: "sample".to_string(),
				gr: "sample".to_string(),
				ns: "sample".to_string(),
			},
		),
		(
			"AccessDbNotFound",
			CatalogError::AccessDbNotFound {
				ac: "sample".to_string(),
				ns: "sample".to_string(),
				db: "sample".to_string(),
			},
		),
		(
			"AccessGrantDbNotFound",
			CatalogError::AccessGrantDbNotFound {
				ac: "sample".to_string(),
				gr: "sample".to_string(),
				ns: "sample".to_string(),
				db: "sample".to_string(),
			},
		),
		(
			"ApAlreadyExists",
			CatalogError::ApAlreadyExists {
				value: "sample".to_string(),
			},
		),
		(
			"AzAlreadyExists",
			CatalogError::AzAlreadyExists {
				name: "sample".to_string(),
			},
		),
		(
			"BuAlreadyExists",
			CatalogError::BuAlreadyExists {
				value: "sample".to_string(),
			},
		),
		(
			"DbAlreadyExists",
			CatalogError::DbAlreadyExists {
				name: "sample".to_string(),
			},
		),
		(
			"EvAlreadyExists",
			CatalogError::EvAlreadyExists {
				name: "sample".to_string(),
			},
		),
		(
			"FdAlreadyExists",
			CatalogError::FdAlreadyExists {
				name: "sample".to_string(),
			},
		),
		(
			"FcAlreadyExists",
			CatalogError::FcAlreadyExists {
				name: "sample".to_string(),
			},
		),
		(
			"MdAlreadyExists",
			CatalogError::MdAlreadyExists {
				name: "sample".to_string(),
			},
		),
		(
			"IxAlreadyExists",
			CatalogError::IxAlreadyExists {
				name: "sample".to_string(),
			},
		),
		(
			"MlAlreadyExists",
			CatalogError::MlAlreadyExists {
				name: "sample".to_string(),
			},
		),
		(
			"NsAlreadyExists",
			CatalogError::NsAlreadyExists {
				name: "sample".to_string(),
			},
		),
		(
			"PaAlreadyExists",
			CatalogError::PaAlreadyExists {
				name: "sample".to_string(),
			},
		),
		(
			"CgAlreadyExists",
			CatalogError::CgAlreadyExists {
				name: "sample".to_string(),
			},
		),
		(
			"SeqAlreadyExists",
			CatalogError::SeqAlreadyExists {
				name: "sample".to_string(),
			},
		),
		(
			"TbAlreadyExists",
			CatalogError::TbAlreadyExists {
				name: "sample".to_string(),
			},
		),
		(
			"UserRootAlreadyExists",
			CatalogError::UserRootAlreadyExists {
				name: "sample".to_string(),
			},
		),
		(
			"UserNsAlreadyExists",
			CatalogError::UserNsAlreadyExists {
				name: "sample".to_string(),
				ns: "sample".to_string(),
			},
		),
		(
			"UserDbAlreadyExists",
			CatalogError::UserDbAlreadyExists {
				name: "sample".to_string(),
				ns: "sample".to_string(),
				db: "sample".to_string(),
			},
		),
		(
			"AccessRootAlreadyExists",
			CatalogError::AccessRootAlreadyExists {
				ac: "sample".to_string(),
			},
		),
		(
			"AccessNsAlreadyExists",
			CatalogError::AccessNsAlreadyExists {
				ac: "sample".to_string(),
				ns: "sample".to_string(),
			},
		),
		(
			"AccessDbAlreadyExists",
			CatalogError::AccessDbAlreadyExists {
				ac: "sample".to_string(),
				ns: "sample".to_string(),
				db: "sample".to_string(),
			},
		),
		(
			"AzInUse",
			CatalogError::AzInUse {
				name: "sample".to_string(),
				table: "sample".to_string(),
				index: "sample".to_string(),
			},
		),
		(
			"ApMethodDuplicate",
			CatalogError::ApMethodDuplicate {
				value: "sample".to_string(),
				method: "sample".to_string(),
			},
		),
		(
			"IndexRebuildRequired",
			CatalogError::IndexRebuildRequired {
				index: "sample".to_string(),
				table: "sample".to_string(),
				expected: 1,
				actual: 1,
			},
		),
	]
}

/// One instance of every [`crate::exec::Error`] variant.
fn every_exec_variant() -> Vec<(&'static str, ExecError)> {
	vec![
		("Thrown", ExecError::Thrown("sample".to_string())),
		("NsEmpty", ExecError::NsEmpty),
		("DbEmpty", ExecError::DbEmpty),
		(
			"Query",
			ExecError::Query {
				message: "sample".to_string(),
			},
		),
		(
			"InvalidParam",
			ExecError::InvalidParam {
				name: "sample".to_string(),
			},
		),
		(
			"InvalidFetch",
			ExecError::InvalidFetch {
				value: Expr::Literal(crate::expr::Literal::None),
			},
		),
		(
			"InvalidLimit",
			ExecError::InvalidLimit {
				value: "sample".to_string(),
			},
		),
		(
			"InvalidStart",
			ExecError::InvalidStart {
				value: "sample".to_string(),
			},
		),
		(
			"InvalidScript",
			ExecError::InvalidScript {
				message: "sample".to_string(),
			},
		),
		(
			"InvalidModel",
			ExecError::InvalidModel {
				message: "sample".to_string(),
			},
		),
		(
			"InvalidFunction",
			ExecError::InvalidFunction {
				name: "sample".to_string(),
				message: "sample".to_string(),
			},
		),
		(
			"InvalidMethodArguments",
			ExecError::InvalidMethodArguments {
				name: "sample".to_string(),
				message: "sample".to_string(),
			},
		),
		(
			"InvalidAggregation",
			ExecError::InvalidAggregation {
				message: "sample".to_string(),
			},
		),
		(
			"InvalidAggregationSelector",
			ExecError::InvalidAggregationSelector {
				expr: "sample".to_string(),
			},
		),
		("InvalidControlFlow", ExecError::InvalidControlFlow),
		(
			"NsNotAllowed",
			ExecError::NsNotAllowed {
				ns: "sample".to_string(),
			},
		),
		(
			"DbNotAllowed",
			ExecError::DbNotAllowed {
				db: "sample".to_string(),
			},
		),
		("ComputationDepthExceeded", ExecError::ComputationDepthExceeded),
		("InvalidStatement", ExecError::InvalidStatement("sample".to_string())),
		(
			"InvalidStatementTarget",
			ExecError::InvalidStatementTarget {
				value: "sample".to_string(),
			},
		),
		(
			"CreateStatement",
			ExecError::CreateStatement {
				value: "sample".to_string(),
			},
		),
		(
			"UpsertStatement",
			ExecError::UpsertStatement {
				value: "sample".to_string(),
			},
		),
		(
			"UpdateStatement",
			ExecError::UpdateStatement {
				value: "sample".to_string(),
			},
		),
		(
			"RelateStatementIn",
			ExecError::RelateStatementIn {
				value: "sample".to_string(),
			},
		),
		(
			"RelateStatementId",
			ExecError::RelateStatementId {
				value: "sample".to_string(),
			},
		),
		(
			"RelateStatementOut",
			ExecError::RelateStatementOut {
				value: "sample".to_string(),
			},
		),
		(
			"DeleteStatement",
			ExecError::DeleteStatement {
				value: "sample".to_string(),
			},
		),
		(
			"InsertStatement",
			ExecError::InsertStatement {
				value: "sample".to_string(),
			},
		),
		(
			"InsertStatementIn",
			ExecError::InsertStatementIn {
				value: "sample".to_string(),
			},
		),
		(
			"InsertStatementId",
			ExecError::InsertStatementId {
				value: "sample".to_string(),
			},
		),
		(
			"InsertStatementOut",
			ExecError::InsertStatementOut {
				value: "sample".to_string(),
			},
		),
		(
			"LiveStatement",
			ExecError::LiveStatement {
				value: "sample".to_string(),
			},
		),
		(
			"KillStatement",
			ExecError::KillStatement {
				value: "sample".to_string(),
			},
		),
		("SingleOnlyOutput", ExecError::SingleOnlyOutput),
		(
			"ParamPermissions",
			ExecError::ParamPermissions {
				name: "sample".to_string(),
			},
		),
		(
			"FunctionPermissions",
			ExecError::FunctionPermissions {
				name: "sample".to_string(),
			},
		),
		("PermissionPredicateSideEffect", ExecError::PermissionPredicateSideEffect),
		(
			"PermissionClauseNotReadonly",
			ExecError::PermissionClauseNotReadonly {
				kind: "sample",
				name: "sample".to_string(),
			},
		),
		(
			"SetCoerce",
			ExecError::SetCoerce {
				name: "sample".to_string(),
				error: Box::new(sample_coerce_error()),
			},
		),
		(
			"ReturnCoerce",
			ExecError::ReturnCoerce {
				name: "sample".to_string(),
				error: Box::new(sample_coerce_error()),
			},
		),
		("Unimplemented", ExecError::Unimplemented("sample".to_string())),
		("PlannerUnsupported", ExecError::PlannerUnsupported("sample".to_string())),
		("PlannerUnimplemented", ExecError::PlannerUnimplemented("sample".to_string())),
		("AccessLevelMismatch", ExecError::AccessLevelMismatch),
		("AccessUnsupportedAlgorithm", ExecError::AccessUnsupportedAlgorithm),
		("AccessRecordTokenDurationRequired", ExecError::AccessRecordTokenDurationRequired),
		("AccessGrantInvalidSubject", ExecError::AccessGrantInvalidSubject),
		("AccessGrantRevoked", ExecError::AccessGrantRevoked),
		(
			"InvalidBound",
			ExecError::InvalidBound {
				found: "sample".to_string(),
				expected: "sample".to_string(),
			},
		),
		(
			"IdiomRecursionLimitExceeded",
			ExecError::IdiomRecursionLimitExceeded {
				limit: 1,
			},
		),
		("UnsupportedRepeatRecurse", ExecError::UnsupportedRepeatRecurse),
		("RecursionInstructionPlanConflict", ExecError::RecursionInstructionPlanConflict),
		(
			"InvalidRecursionTarget",
			ExecError::InvalidRecursionTarget {
				value: "sample".to_string(),
			},
		),
		("ReferenceTypeConflict", ExecError::ReferenceTypeConflict("sample".to_string())),
		("ReferenceNestedField", ExecError::ReferenceNestedField("sample".to_string())),
		(
			"MismatchedFieldTypes",
			ExecError::MismatchedFieldTypes {
				name: "sample".to_string(),
				kind: "sample".to_string(),
				existing_name: "sample".to_string(),
				existing_kind: "sample".to_string(),
			},
		),
		("ComputedKeywordConflict", ExecError::ComputedKeywordConflict("sample".to_string())),
		(
			"ComputedNestedFieldConflict",
			ExecError::ComputedNestedFieldConflict("sample".to_string(), "sample".to_string()),
		),
		(
			"ComputedParentFieldConflict",
			ExecError::ComputedParentFieldConflict("sample".to_string(), "sample".to_string()),
		),
		("ComputedNestedField", ExecError::ComputedNestedField("sample".to_string())),
		("ComputedFieldCycle", ExecError::ComputedFieldCycle("sample".to_string())),
		("IdFieldKeywordConflict", ExecError::IdFieldKeywordConflict("sample".to_string())),
		("IdFieldUnsupportedKind", ExecError::IdFieldUnsupportedKind("sample".to_string())),
		(
			"ComputedFieldCannotBeIndexed",
			ExecError::ComputedFieldCannotBeIndexed {
				field: "sample".to_string(),
				index: "sample".to_string(),
			},
		),
	]
}

fn render_snapshot() -> String {
	let mut out = String::new();
	for (name, error) in every_variant() {
		let mapped = error.to_types_error();
		// The `SurrealValue` form IS the wire form: it carries `code`, `kind`,
		// `details`, `message` and `cause` in one string.
		out.push_str(&format!("{name}\t{}\n", mapped.into_value().to_sql()));
	}
	for (name, error) in every_capabilities_variant() {
		let mapped = error.to_types_error();
		out.push_str(&format!("{name}\t{}\n", mapped.into_value().to_sql()));
	}
	for (name, error) in every_sort_variant() {
		let mapped = error.to_types_error();
		out.push_str(&format!("{name}\t{}\n", mapped.into_value().to_sql()));
	}
	for (name, error) in every_parse_variant() {
		let mapped = error.to_types_error();
		out.push_str(&format!("{name}\t{}\n", mapped.into_value().to_sql()));
	}
	for (name, error) in every_api_variant() {
		let mapped = error.to_types_error();
		out.push_str(&format!("{name}\t{}\n", mapped.into_value().to_sql()));
	}
	for (name, error) in every_engine_variant() {
		let mapped = error.to_types_error();
		out.push_str(&format!("{name}\t{}\n", mapped.into_value().to_sql()));
	}
	for (name, error) in every_key_variant() {
		let mapped = error.to_types_error();
		out.push_str(&format!("{name}\t{}\n", mapped.into_value().to_sql()));
	}
	for (name, error) in every_expr_variant() {
		let mapped = error.to_types_error();
		out.push_str(&format!("{name}\t{}\n", mapped.into_value().to_sql()));
	}
	for (name, error) in every_idx_variant() {
		let mapped = error.to_types_error();
		out.push_str(&format!("{name}\t{}\n", mapped.into_value().to_sql()));
	}
	for (name, error) in every_datastore_variant() {
		let mapped = error.to_types_error();
		out.push_str(&format!("{name}\t{}\n", mapped.into_value().to_sql()));
	}
	for (name, error) in every_buc_variant() {
		let mapped = error.to_types_error();
		out.push_str(&format!("{name}\t{}\n", mapped.into_value().to_sql()));
	}
	for (name, error) in every_doc_variant() {
		let mapped = error.to_types_error();
		out.push_str(&format!("{name}\t{}\n", mapped.into_value().to_sql()));
	}
	for (name, error) in every_catalog_variant() {
		let mapped = error.to_types_error();
		out.push_str(&format!("{name}\t{}\n", mapped.into_value().to_sql()));
	}
	for (name, error) in every_auth_variant() {
		let mapped = error.to_types_error();
		out.push_str(&format!("{name}\t{}\n", mapped.into_value().to_sql()));
	}
	for (name, error) in every_exec_variant() {
		let mapped = error.to_types_error();
		out.push_str(&format!("{name}\t{}\n", mapped.into_value().to_sql()));
	}
	out
}

#[test]
fn wire_mapping_is_unchanged() {
	let actual = render_snapshot();
	let expected = include_str!("wire_snapshot.txt");
	if actual != expected {
		let path = concat!(env!("CARGO_MANIFEST_DIR"), "/src/err/wire_snapshot.actual.txt");
		std::fs::write(path, &actual).expect("write actual snapshot");
		panic!(
			"the public shape of at least one error variant changed.\n\
			 Review the diff, and if the change is intended replace\n\
			 src/err/wire_snapshot.txt with src/err/wire_snapshot.actual.txt.",
		);
	}
}

/// How many variants an error enum declares, read from its own source.
///
/// Derived rather than written down. A fixed number cannot tell "a variant was
/// added and sampled" from "a variant was added and forgotten"; both leave it
/// stale. This is the closest thing to compile-time exhaustiveness available
/// without a derive.
fn declared_variants(source: &str, enum_name: &str) -> usize {
	let header = format!("enum {enum_name} {{");
	source
		.lines()
		.skip_while(|line| !(line.starts_with("pub") && line.ends_with(&header)))
		.skip(1)
		.take_while(|line| *line != "}")
		.filter(|line| {
			// A variant is `\tName`, then its payload, a comma, or end of line.
			// Attributes, doc comments and payload fields are indented further.
			let Some(rest) = line.strip_prefix('\t') else {
				return false;
			};
			if !rest.starts_with(|c: char| c.is_ascii_uppercase()) {
				return false;
			}
			let name = rest.find(|c: char| !c.is_ascii_alphanumeric()).unwrap_or(rest.len());
			// Struct variants put a space before the brace: `\tName {`.
			let tail = rest[name..].trim_start();
			tail.is_empty() || tail.starts_with(['{', '(', ','])
		})
		.count()
}

/// Every error type in the snapshot, paired with the source it is declared in.
///
/// Each entry is `(sampled, declared, label)`. Checking only core's enum would
/// leave the other thirteen unguarded, which is not hypothetical: a variant
/// added to `DatastoreError` on `main` reached this branch with no sample and
/// nothing noticed.
fn coverage() -> Vec<(usize, usize, &'static str)> {
	vec![
		(
			every_variant().len(),
			declared_variants(include_str!("mod.rs"), "Error"),
			"core::err::Error",
		),
		(
			every_engine_variant().len(),
			declared_variants(include_str!("engine.rs"), "EngineError"),
			"err::EngineError",
		),
		(
			every_parse_variant().len(),
			declared_variants(include_str!("../../../syn/src/lib.rs"), "ParseError"),
			"syn::ParseError",
		),
		(
			every_key_variant().len(),
			declared_variants(include_str!("../key/error.rs"), "Error"),
			"key::Error",
		),
		(
			every_catalog_variant().len(),
			declared_variants(include_str!("../catalog/error.rs"), "Error"),
			"catalog::Error",
		),
		(
			every_doc_variant().len(),
			declared_variants(include_str!("../doc/error.rs"), "Error"),
			"doc::Error",
		),
		(
			every_buc_variant().len(),
			declared_variants(include_str!("../buc/error.rs"), "Error"),
			"buc::Error",
		),
		(
			every_idx_variant().len(),
			declared_variants(include_str!("../idx/error.rs"), "Error"),
			"idx::Error",
		),
		(
			every_expr_variant().len(),
			declared_variants(include_str!("../expr/error.rs"), "Error"),
			"expr::Error",
		),
		(
			every_exec_variant().len(),
			declared_variants(include_str!("../exec/error.rs"), "Error"),
			"exec::Error",
		),
		(
			every_auth_variant().len(),
			declared_variants(include_str!("../iam/error.rs"), "Error"),
			"iam::Error",
		),
		(
			every_datastore_variant().len(),
			declared_variants(include_str!("../kvs/datastore_error.rs"), "DatastoreError"),
			"kvs::DatastoreError",
		),
		(
			every_sort_variant().len(),
			declared_variants(include_str!("../dbs/sort_error.rs"), "SortError"),
			"dbs::SortError",
		),
		(
			every_capabilities_variant().len(),
			declared_variants(include_str!("../dbs/capabilities/error.rs"), "Error"),
			"capabilities::Error",
		),
		(
			every_api_variant().len(),
			declared_variants(include_str!("../api/err.rs"), "ApiError"),
			"ApiError",
		),
	]
}

#[test]
fn snapshot_covers_every_variant() {
	let gaps: Vec<_> = coverage()
		.into_iter()
		.filter(|(sampled, declared, _)| sampled != declared)
		.map(|(sampled, declared, label)| {
			format!("{label}: {sampled} sampled, {declared} declared")
		})
		.collect();
	assert!(
		gaps.is_empty(),
		"every variant needs a sample, or it is absent from the snapshot and its public \
		 shape is unguarded: {gaps:#?}",
	);
}

/// How many variants reach clients as an untyped `Internal` error.
///
/// These carry a message and nothing a client can branch on. A handful are a
/// deliberate choice - see the `TypesError::internal` arms - but most are
/// unclassified [`common::internal_todo`] sites, each a candidate for a real
/// kind. The assertion is one-directional on purpose:
/// the number may fall whenever a variant is classified properly, but it must
/// never rise, because a rise means structure was lost.
const UNTYPED_INTERNAL_BUDGET: usize = 168;

/// Counts variants whose OWN kind is `Internal`.
///
/// Reads the structured value rather than grepping the rendered snapshot. Any
/// variant carrying a source attaches that source as a `cause`, and a cause
/// renders a nested `kind: 'Internal'` of its own, so a text filter counts
/// properly-classified rows such as `Coerce`, `Revision` and `IamError` as
/// untyped. That would inflate the budget and leave classifying one of them
/// unable to move the number the ratchet exists to move.
fn untyped_internal_count() -> usize {
	let core = every_variant().into_iter().map(|(_, e)| e.to_types_error());
	let capabilities = every_capabilities_variant().into_iter().map(|(_, e)| e.to_types_error());
	let sort = every_sort_variant().into_iter().map(|(_, e)| e.to_types_error());
	let parse = every_parse_variant().into_iter().map(|(_, e)| e.to_types_error());
	let api = every_api_variant().into_iter().map(|(_, e)| e.to_types_error());
	let engine = every_engine_variant().into_iter().map(|(_, e)| e.to_types_error());
	let key = every_key_variant().into_iter().map(|(_, e)| e.to_types_error());
	let expr = every_expr_variant().into_iter().map(|(_, e)| e.to_types_error());
	let idx = every_idx_variant().into_iter().map(|(_, e)| e.to_types_error());
	let datastore = every_datastore_variant().into_iter().map(|(_, e)| e.to_types_error());
	let buc = every_buc_variant().into_iter().map(|(_, e)| e.to_types_error());
	let doc = every_doc_variant().into_iter().map(|(_, e)| e.to_types_error());
	let catalog = every_catalog_variant().into_iter().map(|(_, e)| e.to_types_error());
	let auth = every_auth_variant().into_iter().map(|(_, e)| e.to_types_error());
	let exec = every_exec_variant().into_iter().map(|(_, e)| e.to_types_error());
	core.chain(capabilities)
		.chain(sort)
		.chain(parse)
		.chain(api)
		.chain(engine)
		.chain(key)
		.chain(expr)
		.chain(idx)
		.chain(datastore)
		.chain(buc)
		.chain(doc)
		.chain(catalog)
		.chain(auth)
		.chain(exec)
		.filter(|mapped| mapped.kind_str() == "Internal")
		.count()
}

#[test]
fn untyped_internal_errors_never_increase() {
	let untyped = untyped_internal_count();
	assert!(
		untyped <= UNTYPED_INTERNAL_BUDGET,
		"{untyped} variants now reach clients as an untyped internal error, up from \
		 {UNTYPED_INTERNAL_BUDGET}. Give the new one a kind rather than raising the budget.",
	);
	assert_eq!(
		untyped, UNTYPED_INTERNAL_BUDGET,
		"{untyped} variants reach clients untyped; lower UNTYPED_INTERNAL_BUDGET to match.",
	);
}
