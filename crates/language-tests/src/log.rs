use std::sync::RwLock;

use tracing::Level;
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};

static LOG_COLLECTORS: RwLock<Vec<Arc<RwLock<LogCollector>>>> = RwLock::new(Vec::new());

thread_local! {
    pub static LOCAL: Arc<RwLock<LogCollector>> = {
    }
}

pub fn filter_logs(filter: Fn(

pub fn init(level: Level) {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .without_time()
        .with_ansi(atty::is(atty::Stream::Stdout))
        .with_target(false);

    let filter_layer = tracing_subscriber::filter::dynamic_filter_fn(move |metadata, context| {
        *metadata.level() <= level
            && context
                .current_span()
                .metadata()
                .map(|x| x.name() != "initialize_reused_stores")
                .unwrap_or(true)
    });

    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .init();
}
