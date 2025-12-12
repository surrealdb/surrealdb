use std::sync::LazyLock;

use semver::VersionReq;

pub enum Feature {
    LiveQueries,
    Transactions,
    Sessions,
    RefreshTokens,
}

static SINCE_0_0_0: LazyLock<VersionReq> =
	LazyLock::new(|| VersionReq::parse(">=0.0.0").expect("valid version requirement"));
static SINCE_3_0_0: LazyLock<VersionReq> =
	LazyLock::new(|| VersionReq::parse(">=3.0.0").expect("valid version requirement"));

impl Feature {
    pub fn version_requirement(&self) -> VersionReq {
        match self {
            Feature::LiveQueries => SINCE_0_0_0.clone(),
            Feature::Transactions => SINCE_3_0_0.clone(),
            Feature::Sessions => SINCE_3_0_0.clone(),
            Feature::RefreshTokens => SINCE_3_0_0.clone(),
        }
    }
}

pub trait FeatureSupport {
    fn supports_feature(&self, feature: Feature) -> bool;
}