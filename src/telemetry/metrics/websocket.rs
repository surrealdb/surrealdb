// use std::time::Duration;
// use http::Version;
// use opentelemetry::{metrics::{Meter, Unit, Histogram, ObservableUpDownCounter, MetricsError}, KeyValue, Context};
// use once_cell::sync::Lazy;

// pub static HISTOGRAM_BOUNDARIES: [f64; 10] = [1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 200.0, 500.0, 1000.0, 5000.0];

// impl Http

// pub fn on_request_start(flavor: Version, method: &str, scheme: Option<&http::uri::Scheme>, host: Option<&str>, target: Option<&str>) -> Result<(), MetricsError> {
//     // Setup the active_requests observer
//     observe_active_request_start(flavor, method, scheme, host, target)
// }

// pub fn on_request_finish(flavor: Version, method: &str, scheme: Option<&http::uri::Scheme>, host: Option<&str>, target: Option<&str>, status: &str, duration: Duration) -> Result<(), MetricsError> {
//     // Setup the active_requests observer
//     observe_active_request_finish(flavor, method, scheme, host, target)?;

//     // Record the duration of the request.
//     record_request_duration(flavor, method, scheme, host, target, status, duration);

//     Ok(())
// }

// fn observe_active_request_start(flavor: Version, method: &str, scheme: Option<&http::uri::Scheme>, host: Option<&str>, target: Option<&str>) -> Result<(), MetricsError> {
//     // Build metric attributes.
//     let mut attrs = vec![
//         KeyValue::new("http.method", method.to_owned()),
//         KeyValue::new("http.flavor", format!("{flavor:?}")),
//     ];
    
//     if let Some(scheme) = scheme {
//         attrs.push(KeyValue::new("http.scheme", scheme.as_str().to_owned()));
//     }
    
//     if let Some(host) = host {
//         attrs.push(KeyValue::new("http.host", host.to_owned()));
//     }

//     if let Some(target) = target {
//         attrs.push(KeyValue::new("http.target", target.to_owned()));
//     }

//     // Setup the callback to observe the active requests.
//     HTTP_METER.register_callback(move |ctx| {
//         HTTP_SERVER_ACTIVE_REQUESTS.observe(ctx, 1,&attrs)
//     }) 
// }

// fn observe_active_request_finish(flavor: Version, method: &str, scheme: Option<&http::uri::Scheme>, host: Option<&str>, target: Option<&str>) -> Result<(), MetricsError> {
//     // Prepare the metric attributes.
//     // Build metric attributes.
//     let mut attrs = vec![
//         KeyValue::new("http.method", method.to_owned()),
//         KeyValue::new("http.flavor", format!("{flavor:?}")),
//     ];
    
//     if let Some(scheme) = scheme {
//         attrs.push(KeyValue::new("http.scheme", scheme.as_str().to_owned()));
//     }
    
//     if let Some(host) = host {
//         attrs.push(KeyValue::new("http.host", host.to_owned()));
//     }
    
//     if let Some(target) = target {
//         attrs.push(KeyValue::new("http.target", target.to_owned()));
//     }

//     // Setup the callback to observe the active requests.
//     HTTP_METER.register_callback(move |ctx| {
//         HTTP_SERVER_ACTIVE_REQUESTS.observe(ctx, -1,&attrs)
//     }) 
// }

// fn record_request_duration(flavor: Version, method: &str, scheme: Option<&http::uri::Scheme>, host: Option<&str>, target: Option<&str>, status: &str, duration: Duration) {
//     // Build metric attributes.
//     let mut attrs = vec![
//         KeyValue::new("http.method", method.to_owned()),
//         KeyValue::new("http.status_code", status.to_owned()),
//         KeyValue::new("http.flavor", format!("{flavor:?}")),
//     ];
    
//     if let Some(scheme) = scheme {
//         attrs.push(KeyValue::new("http.scheme", scheme.as_str().to_owned()));
//     }
    
//     if let Some(host) = host {
//         attrs.push(KeyValue::new("http.host", host.to_owned()));
//     }

//     if let Some(target) = target {
//         attrs.push(KeyValue::new("http.target", target.to_owned()));
//     }

//     // Record the duration of the request.
//     HTTP_SERVER_DURATION.record(&Context::current(), duration.as_millis() as u64, &attrs);
// }


// pub static HTTP_METER: Lazy<Meter> = Lazy::new(|| opentelemetry::global::meter("http"));

// pub static HTTP_SERVER_DURATION: Lazy<Histogram<u64>> = Lazy::new(|| {
//     HTTP_METER
//         .u64_histogram("server.duration")
//         .with_description("The HTTP server duration in milliseconds.")
//         .with_unit(Unit::new("ms"))
//         .init()
// });

// pub static HTTP_SERVER_ACTIVE_REQUESTS: Lazy<ObservableUpDownCounter<i64>> = Lazy::new(|| {
//     HTTP_METER
//         .i64_observable_up_down_counter("server.active_requests")
//         .with_description("The number of active HTTP requests.")
//         .init()
// });
