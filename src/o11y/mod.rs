pub mod filters;
pub mod tracers;

use std::env;

use tracing::Level;
use tracing_subscriber::prelude::*;

const TRACING_TRACER_VAR: &str = "SURREAL_TRACING_TRACER";

#[derive(Default, Debug, Clone)]
pub struct Builder {
    log_filter: String,
}

pub fn builder() -> Builder {
    Builder::default()
}

impl Builder {
    /// Translates the given log_level into a log_filter
    pub fn with_log_level(self, log_level: &str) -> Self {
        self.with_log_filter(
            match log_level {
                "warn" | "info" | "debug" | "trace" => {
                    let level: Level = log_level.parse().unwrap();

                    format!("error,surreal={},surrealdb={},surrealdb::txn={}", level, level, level)
                },
                "full" => {
                    Level::TRACE.to_string()
                },
                _ => unreachable!(),
            }
        )
    }

    pub fn with_log_filter(mut self, filter: String) -> Self {
        self.log_filter = filter;
        self
    }

    /// Setup the global tracing with the fmt subscriber (logs) and the chosen tracer subscriber
    pub fn init(self) {
        let tracing_registry = tracing_subscriber::registry()
            // Create the fmt subscriber for printing the tracing Events as logs to the stdout
            .with(
                tracing_subscriber::fmt::layer().with_filter(filters::fmt(self.log_filter))
            );
        
        // Init the tracing_registry with the selected tracer. If no tracer is provided, init without one
        match env::var(TRACING_TRACER_VAR).unwrap_or_default().trim().to_ascii_lowercase().as_str() {
            // If no tracer is selected, init with the fmt subscriber only
            "noop" | "" => {
                tracing_registry.init();
                debug!("No tracer defined");
            },
            // Init the registry with the OTLP tracer
            "otlp" => {
                tracing_registry
                    .with(
                        tracing_opentelemetry::layer()
                            .with_tracer(tracers::oltp().unwrap())
                            .with_filter(filters::otlp())
                    )
                    .init();
                debug!("OTLP tracer setup");
            },
            tracer => {
                panic!("unsupported tracer {}", tracer);
            }
        };
    }
}

