//! Performant cross-platform timing with goodies.
//!
//! `quanta` provides a simple and fast API for measuring the current time and the duration between
//! events.  It does this by providing a thin layer on top of native OS timing functions, or, if
//! available, using the Time Stamp Counter feature found on modern CPUs.
//!
//! # Design
//!
//! Internally, `quanta` maintains the concept of two potential clock sources: a reference clock and
//! a source clock.
//!
//! The reference clock is provided by the OS, and always available.  It is equivalent to what is
//! provided by the standard library in terms of the underlying system calls being made.  As it uses
//! the native timing facilities provided by the operating system, we ultimately depend on the OS
//! itself to give us a stable and correct value.
//!
//! The source clock is a potential clock source based on the [Time Stamp Counter][tsc] feature
//! found on modern CPUs.  If the TSC feature is not present or is not reliable enough, `quanta`
//! will transparently utilize the reference clock instead.
//!
//! Depending on the underlying processor(s) in the system, `quanta` will figure out the most
//! accurate/efficient way to calibrate the source clock to the reference clock in order to provide
//! measurements scaled to wall clock time.
//!
//! Details on TSC support, and calibration, are detailed below.
//!
//! # Features
//!
//! Beyond simply taking measurements of the current time, `quanta` provides features for more
//! easily working with clocks, as well as being able to enhance performance further:
//! - `Clock` can be mocked for testing
//! - globally accessible "recent" time with amortized overhead
//!
//! ## Mocked time
//!
//! For any code that uses a `Clock`, a mocked version can be substituted.  This allows for
//! application authors to control the time in tests, which allows simulating not only the normal
//! passage of time but provides the ability to warp time forwards and backwards in order to test
//! corner cases in logic, etc.  Creating a mocked clock can be acheived with [`Clock::mock`], and
//! [`Mock`] contains more details on mock usage.
//!
//! ## Coarsely-updated, or recent, time
//!
//! `quanta` also provides a "recent" time feature, which allows a slightly-delayed version of time
//! to be provided to callers, trading accuracy for speed of access.  An upkeep thread is spawned,
//! which is responsible for taking measurements and updating the global recent time. Callers then
//! can access the cached value by calling `Clock::recent`.  This interface can be 4-10x faster than
//! directly calling `Clock::now`, even when TSC support is available.  As the upkeep thread is the
//! only code updating the recent time, the accuracy of the value given to callers is limited by how
//! often the upkeep thread updates the time, thus the trade off between accuracy and speed of
//! access.
//!
//! # Feature Flags
//!
//! `quanta` comes with feature flags that enable convenient conversions to time types in other
//! popular crates, such as:
//! - `prost` - provides an implementation into [`Timestamp`][prost_types_timestamp] from
//!   `prost_types`
//!
//! # Platform Support
//!
//! At a high level, `quanta` carries support for most major operating systems out of the box:
//! - Windows ([`QueryPerformanceCounter`][QueryPerformanceCounter])
//! - macOS/OS X/iOS ([`mach_absolute_time`][mach_absolute_time])
//! - Linux/*BSD/Solaris ([`clock_gettime`][clock_gettime])
//!
//! These platforms are supported in the "reference" clock sense, and support for using the Time
//! Stamp Counter as a clocksource is more subtle, and explained below.
//!
//! ## WASM support
//!
//! This library can be built for WASM targets, but in this case the resolution and accuracy of
//! measurements can be limited by the WASM environment. In particular, when running on the
//! `wasm32-unknown-unknown` target in browsers, `quanta` will use [window.performance.now] as a
//! clock. This mean the accuracy is limited to milliseconds instead of the usual nanoseconds on
//! other targets. When running within a WASI environment (target `wasm32-wasi`), the accuracy of
//! the clock depends on the VM implementation.
//!
//! # TSC Support
//!
//! Accessing the TSC requires being on the `x86_64` architecture, with access to SSE2.
//! Additionally, the processor must support either constant or nonstop/invariant TSC.  This ensures
//! that the TSC ticks at a constant rate which can be easily scaled.
//!
//! A caveat is that "constant" TSC doesn't account for all possible power states (levels of power
//! down or sleep that a CPU can enter to save power under light load, etc) and so a constant TSC
//! can lead to drift in measurements over time, after they've been scaled to reference time.
//!
//! This is a limitation of the TSC mode, as well as the nature of `quanta` not being able to know,
//! as the OS would, when a power state transition has happened, and thus compensate with a
//! recalibration. Nonstop/invariant TSC does not have this limitation and is stable over long
//! periods of time.
//!
//! Roughly speaking, the following list contains the beginning model/generation of processors where
//! you should be able to expect having invariant TSC support:
//! - Intel Nehalem and newer for server-grade
//! - Intel Skylake and newer for desktop-grade
//! - VIA Centaur Nano and newer (circumstantial evidence here)
//! - AMD Phenom and newer
//!
//! Ultimately, `quanta` will query CPUID information to determine if the processor has the required
//! features to use the TSC.
//!
//! # Calibration
//!
//! As the TSC doesn't necessarily tick at reference scale -- i.e. one tick isn't always one
//! nanosecond -- we have to apply a scaling factor when converting from source to reference time
//! scale to provide this.  We acquire this scaling factor by repeatedly taking measurements from
//! both the reference and source clocks, until we have a statistically-relevant measure of the
//! average scaling factor.  We do some additional work to convert this scaling factor into a
//! power-of-two number that allows us to optimize the code, and thus reduce the generated
//! instructions required to scale a TSC value.
//!
//! This calibration is stored globally and reused.  However, the first `Clock` that is created in
//! an application will block for a small period of time as it runs this calibration loop.  The time
//! spent in the calibration loop is limited to 200ms overall.  In practice, `quanta` will reach a
//! stable calibration quickly (usually 10-20ms, if not less) and so this deadline is unlikely to be
//! reached.
//!
//! # Caveats
//!
//! Utilizing the TSC can be a tricky affair, and so here is a list of caveats that may or may not
//! apply, and is in no way exhaustive:
//! - CPU hotplug behavior is undefined
//! - raw values may time warp
//! - measurements from the TSC may drift past or behind the comparable reference clock
//!
//! Another important caveat is that `quanta` does not track time across system suspends.  Simply
//! put, if a time measurement (such as using [`Instant::now`][crate::Instant::now]) is taken, and
//! then the system is suspended, and then another measurement is taken, the difference between
//! those the two would not include the time the system was in suspend.
//!
//! [tsc]: https://en.wikipedia.org/wiki/Time_Stamp_Counter
//! [QueryPerformanceCounter]: https://msdn.microsoft.com/en-us/library/ms644904(v=VS.85).aspx
//! [mach_absolute_time]: https://developer.apple.com/documentation/kernel/1462446-mach_absolute_time
//! [clock_gettime]: https://linux.die.net/man/3/clock_gettime
//! [prost_types_timestamp]: https://docs.rs/prost-types/0.7.0/prost_types/struct.Timestamp.html
//! [window.performance.now]: https://developer.mozilla.org/en-US/docs/Web/API/Performance/now
#![deny(missing_docs)]
#![deny(clippy::all)]
#![allow(clippy::must_use_candidate)]

use crossbeam_utils::atomic::AtomicCell;
use std::time::Duration;
use std::{cell::RefCell, sync::Arc};

use once_cell::sync::OnceCell;

mod clocks;
use self::clocks::{Counter, Monotonic};
mod detection;
mod mock;
pub use self::mock::{IntoNanoseconds, Mock};
mod instant;
pub use self::instant::Instant;
mod upkeep;
pub use self::upkeep::{Error, Handle, Upkeep};
mod stats;
use self::stats::Variance;

// Global clock, used by `Instant::now`.
static GLOBAL_CLOCK: OnceCell<Clock> = OnceCell::new();

// Global recent measurement, used by `Clock::recent` and `Instant::recent`.
static GLOBAL_RECENT: AtomicCell<u64> = AtomicCell::new(0);

// Global calibration, shared by all clocks.
static GLOBAL_CALIBRATION: OnceCell<Calibration> = OnceCell::new();

// Per-thread clock override, used by `quanta::with_clock`, `Instant::now`, and sometimes `Instant::recent`.
thread_local! {
    static CLOCK_OVERRIDE: RefCell<Option<Clock>> = RefCell::new(None);
}

// Run 500 rounds of calibration before we start actually seeing what the numbers look like.
const MINIMUM_CAL_ROUNDS: u64 = 500;

// We want our maximum error to be 10 nanoseconds.
const MAXIMUM_CAL_ERROR_NS: u64 = 10;

// Don't run the calibration loop for longer than 200ms of wall time.
const MAXIMUM_CAL_TIME_NS: u64 = 200 * 1000 * 1000;

#[derive(Debug)]
enum ClockType {
    Monotonic(Monotonic),
    Counter(Monotonic, Counter, Calibration),
    Mock(Arc<Mock>),
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct Calibration {
    ref_time: u64,
    src_time: u64,
    scale_factor: u64,
    scale_shift: u32,
}

impl Calibration {
    fn new() -> Calibration {
        Calibration {
            ref_time: 0,
            src_time: 0,
            scale_factor: 1,
            scale_shift: 0,
        }
    }

    fn reset_timebases(&mut self, reference: Monotonic, source: &Counter) {
        self.ref_time = reference.now();
        self.src_time = source.now();
    }

    fn scale_src_to_ref(&self, src_raw: u64) -> u64 {
        let delta = src_raw.saturating_sub(self.src_time);
        let scaled = mul_div_po2_u64(delta, self.scale_factor, self.scale_shift);
        scaled + self.ref_time
    }

    fn calibrate(&mut self, reference: Monotonic, source: &Counter) {
        let mut variance = Variance::default();
        let deadline = reference.now() + MAXIMUM_CAL_TIME_NS;

        self.reset_timebases(reference, source);

        // Each busy loop should spin for 1 microsecond. (1000 nanoseconds)
        let loop_delta = 1000;
        loop {
            // Busy loop to burn some time.
            let mut last = reference.now();
            let target = last + loop_delta;
            while last < target {
                last = reference.now();
            }

            // We put an upper bound on how long we run calibration before to provide a predictable
            // overhead to the calibration process.  In practice, even if we hit the calibration
            // deadline, we should still have run a sufficient number of rounds to get an accurate
            // calibration.
            if last >= deadline {
                break;
            }

            // Adjust our calibration before we take our measurement.
            self.adjust_cal_ratio(reference, source);

            let r_time = reference.now();
            let s_raw = source.now();
            let s_time = self.scale_src_to_ref(s_raw);
            variance.add(s_time as f64 - r_time as f64);

            // If we've collected enough samples, check what the mean and mean error are.  If we're
            // already within the target bounds, we can break out of the calibration loop early.
            if variance.has_significant_result() {
                let mean = variance.mean().abs();
                let mean_error = variance.mean_error().abs();
                let mwe = variance.mean_with_error();
                let samples = variance.samples();

                if samples > MINIMUM_CAL_ROUNDS
                    && mwe < MAXIMUM_CAL_ERROR_NS as f64
                    && mean_error <= mean
                {
                    break;
                }
            }
        }
    }

    fn adjust_cal_ratio(&mut self, reference: Monotonic, source: &Counter) {
        // Overall algorithm: measure the delta between our ref/src_time values and "now" versions
        // of them, calculate the ratio between the deltas, and then find a numerator and
        // denominator to express that ratio such that the denominator is always a power of two.
        //
        // In practice, this means we take the "source" delta, and find the next biggest number that
        // is a power of two.  We then figure out the ratio that describes the difference between
        // _those_ two values, and multiple the "reference" delta by that much, which becomes our
        // numerator while the power-of-two "source" delta becomes our denominator.
        //
        // Then, conversion from a raw value simply becomes a multiply and a bit shift instead of a
        // multiply and full-blown divide.
        let ref_end = reference.now();
        let src_end = source.now();

        let ref_d = ref_end.wrapping_sub(self.ref_time);
        let src_d = src_end.wrapping_sub(self.src_time);

        let src_d_po2 = src_d
            .checked_next_power_of_two()
            .unwrap_or_else(|| 2_u64.pow(63));

        // TODO: lossy conversion back and forth just to get an approximate value, can we do better
        // with integer math? not sure
        let po2_ratio = src_d_po2 as f64 / src_d as f64;
        self.scale_factor = (ref_d as f64 * po2_ratio) as u64;
        self.scale_shift = src_d_po2.trailing_zeros();
    }
}

impl Default for Calibration {
    fn default() -> Self {
        Self::new()
    }
}

/// Unified clock for taking measurements.
#[derive(Debug, Clone)]
pub struct Clock {
    inner: ClockType,
}

impl Clock {
    /// Creates a new clock with the optimal reference and source clocks.
    ///
    /// Support for TSC, etc, are checked at the time of creation, not compile-time.
    pub fn new() -> Clock {
        let reference = Monotonic::default();
        let inner = if detection::has_counter_support() {
            let source = Counter;
            let calibration = GLOBAL_CALIBRATION.get_or_init(|| {
                let mut calibration = Calibration::new();
                calibration.calibrate(reference, &source);
                calibration
            });
            ClockType::Counter(reference, source, *calibration)
        } else {
            ClockType::Monotonic(reference)
        };

        Clock { inner }
    }

    /// Creates a new clock that is mocked for controlling the underlying time.
    ///
    /// Returns a [`Clock`] instance and a handle to the underlying [`Mock`] source so that the
    /// caller can control the passage of time.
    pub fn mock() -> (Clock, Arc<Mock>) {
        let mock = Arc::new(Mock::new());
        let clock = Clock {
            inner: ClockType::Mock(mock.clone()),
        };

        (clock, mock)
    }

    /// Gets the current time, scaled to reference time.
    ///
    /// This method is the spiritual equivalent of [`std::time::Instant::now`].  It is guaranteed
    /// to return a monotonically increasing value between calls to the same `Clock` instance.
    ///
    /// Returns an [`Instant`].
    pub fn now(&self) -> Instant {
        match &self.inner {
            ClockType::Monotonic(monotonic) => Instant(monotonic.now()),
            ClockType::Counter(_, counter, _) => self.scaled(counter.now()),
            ClockType::Mock(mock) => Instant(mock.value()),
        }
    }

    /// Gets the underlying time from the fastest available clock source.
    ///
    /// As the clock source may or may not be the TSC, value is not guaranteed to be in nanoseconds
    /// or to be monotonic.  Value can be scaled to reference time by calling either [`scaled`]
    /// or [`delta`].
    ///
    /// [`scaled`]: Clock::scaled
    /// [`delta`]: Clock::delta
    pub fn raw(&self) -> u64 {
        match &self.inner {
            ClockType::Monotonic(monotonic) => monotonic.now(),
            ClockType::Counter(_, counter, _) => counter.now(),
            ClockType::Mock(mock) => mock.value(),
        }
    }

    /// Scales a raw measurement to reference time.
    ///
    /// You must scale raw measurements to ensure your result is in nanoseconds.  The raw
    /// measurement is not guaranteed to be in nanoseconds and may vary.  It is only OK to avoid
    /// scaling raw measurements if you don't need actual nanoseconds.
    ///
    /// Returns an [`Instant`].
    pub fn scaled(&self, value: u64) -> Instant {
        let scaled = match &self.inner {
            ClockType::Counter(_, _, calibration) => calibration.scale_src_to_ref(value),
            _ => value,
        };

        Instant(scaled)
    }

    /// Calculates the delta, in nanoseconds, between two raw measurements.
    ///
    /// This method is very similar to [`delta`] but reduces overhead
    /// for high-frequency measurements that work with nanosecond
    /// counts internally, as it avoids the conversion of the delta
    /// into [`Duration`].
    ///
    /// [`delta`]: Clock::delta
    pub fn delta_as_nanos(&self, start: u64, end: u64) -> u64 {
        // Safety: we want wrapping_sub on the end/start delta calculation so that two measurements
        // split across a rollover boundary still return the right result.  However, we also know
        // the TSC could potentially give us different values between cores/sockets, so we're just
        // doing our due diligence here to make sure we're not about to create some wacky duration.
        if end <= start {
            return 0;
        }

        let delta = end.wrapping_sub(start);
        match &self.inner {
            ClockType::Counter(_, _, calibration) => {
                mul_div_po2_u64(delta, calibration.scale_factor, calibration.scale_shift)
            }
            _ => delta,
        }
    }

    /// Calculates the delta between two raw measurements.
    ///
    /// This method is slightly faster when you know you need the delta between two raw
    /// measurements, or a start/end measurement, than using [`scaled`] for both conversions.
    ///
    /// In code that simply needs access to the whole number of nanoseconds
    /// between the two measurements, consider [`Clock::delta_as_nanos`]
    /// instead, which is slightly faster than having to call both this method
    /// and [`Duration::as_nanos`].
    ///
    /// [`scaled`]: Clock::scaled
    /// [`delta_as_nanos`]: Clock::delta_as_nanos
    pub fn delta(&self, start: u64, end: u64) -> Duration {
        Duration::from_nanos(self.delta_as_nanos(start, end))
    }

    /// Gets the most recent current time, scaled to reference time.
    ///
    /// This method provides ultra-low-overhead access to a slightly-delayed version of the current
    /// time.  Instead of querying the underlying source clock directly, a shared, global value is
    /// read directly without the need to scale to reference time.
    ///
    /// The upkeep thread must be started in order to update the time.  You can read the
    /// documentation for [`Upkeep`] for more information on starting the upkeep thread, as
    /// well as the details of the "current time" mechanism.
    ///
    /// If the upkeep thread has not been started, the return value will be `0`.
    ///
    /// Returns an [`Instant`].
    pub fn recent(&self) -> Instant {
        match &self.inner {
            ClockType::Mock(mock) => Instant(mock.value()),
            _ => Instant(GLOBAL_RECENT.load()),
        }
    }

    #[cfg(test)]
    #[allow(dead_code)]
    fn reset_timebase(&mut self) -> bool {
        match &mut self.inner {
            ClockType::Counter(reference, source, calibration) => {
                calibration.reset_timebases(*reference, source);
                true
            }
            _ => false,
        }
    }
}

impl Default for Clock {
    fn default() -> Clock {
        Clock::new()
    }
}

// A manual `Clone` impl is required because `atomic_shim`'s `AtomicU64` is not `Clone`.
impl Clone for ClockType {
    fn clone(&self) -> Self {
        match self {
            ClockType::Mock(mock) => ClockType::Mock(mock.clone()),
            ClockType::Monotonic(monotonic) => ClockType::Monotonic(*monotonic),
            ClockType::Counter(monotonic, counter, calibration) => {
                ClockType::Counter(*monotonic, counter.clone(), *calibration)
            }
        }
    }
}

/// Sets this clock as the default for the duration of a closure.
///
/// This will only affect calls made against [`Instant`].  [`Clock`] is always self-contained.
pub fn with_clock<T>(clock: &Clock, f: impl FnOnce() -> T) -> T {
    CLOCK_OVERRIDE.with(|current| {
        let old = current.replace(Some(clock.clone()));
        let result = f();
        current.replace(old);
        result
    })
}

/// Sets the global recent time.
///
/// While callers should typically prefer to use [`Upkeep`] to establish a background thread in
/// order to drive the global recent time, this function allows callers to customize how the global
/// recent time is updated.  For example, programs using an asynchronous runtime may prefer to
/// schedule a task that does the updating, avoiding an extra thread.
pub fn set_recent(instant: Instant) {
    GLOBAL_RECENT.store(instant.0);
}

#[inline]
pub(crate) fn get_now() -> Instant {
    if let Some(instant) = CLOCK_OVERRIDE.with(|clock| clock.borrow().as_ref().map(Clock::now)) {
        instant
    } else {
        GLOBAL_CLOCK.get_or_init(Clock::new).now()
    }
}

#[inline]
pub(crate) fn get_recent() -> Instant {
    // We make a small trade-off here where if the global recent time isn't zero, we use that,
    // regardless of whether or not there's a thread-specific clock override.  Otherwise, we would
    // blow our performance budget.
    //
    // Given that global recent time shouldn't ever be getting _actually_ updated in tests, this
    // should be a reasonable trade-off.
    let recent = GLOBAL_RECENT.load();
    if recent == 0 {
        get_now()
    } else {
        Instant(recent)
    }
}

#[inline]
fn mul_div_po2_u64(value: u64, numer: u64, denom: u32) -> u64 {
    // Modified muldiv routine where the denominator has to be a power of two. `denom` is expected
    // to be the number of bits to shift, not the actual decimal value.
    let mut v = u128::from(value);
    v *= u128::from(numer);
    v >>= denom;
    v as u64
}

#[cfg(test)]
mod tests {
    use super::Clock;

    #[cfg(not(target_arch = "wasm32"))]
    use super::{Counter, Monotonic};

    #[cfg(not(target_arch = "wasm32"))]
    use average::{Merge as _, Variance};

    #[cfg(not(target_arch = "wasm32"))]
    use std::time::{Duration, Instant};

    #[test]
    #[cfg_attr(
        all(target_arch = "wasm32", target_os = "unknown"),
        wasm_bindgen_test::wasm_bindgen_test
    )]
    fn test_mock() {
        let (clock, mock) = Clock::mock();
        assert_eq!(clock.now().0, 0);
        mock.increment(42);
        assert_eq!(clock.now().0, 42);
    }

    #[test]
    #[cfg_attr(
        all(target_arch = "wasm32", target_os = "unknown"),
        wasm_bindgen_test::wasm_bindgen_test
    )]
    fn test_now() {
        let clock = Clock::new();
        assert!(clock.now().0 > 0);
    }

    #[test]
    #[cfg_attr(
        all(target_arch = "wasm32", target_os = "unknown"),
        wasm_bindgen_test::wasm_bindgen_test
    )]
    fn test_raw() {
        let clock = Clock::new();
        assert!(clock.raw() > 0);
    }

    #[test]
    #[cfg_attr(
        all(target_arch = "wasm32", target_os = "unknown"),
        wasm_bindgen_test::wasm_bindgen_test
    )]
    fn test_scaled() {
        let clock = Clock::new();
        let raw = clock.raw();
        let scaled = clock.scaled(raw);
        assert!(scaled.0 > 0);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    #[cfg_attr(not(feature = "flaky_tests"), ignore)]
    fn test_reference_source_calibration() {
        let mut clock = Clock::new();
        let reference = Monotonic::default();

        let loops = 10000;

        let mut overall = Variance::new();
        let mut src_samples = [0u64; 1024];
        let mut ref_samples = [0u64; 1024];

        for _ in 0..loops {
            // We have to reset the "timebase" of the clock/calibration when testing in this way.
            //
            // Since `quanta` is designed around mimicing `Instant`, we care about measuring the _passage_ of time, but
            // not matching our calculation of wall-clock time to the system's calculation of wall-clock time, in terms
            // of their absolute values.
            //
            // As the system adjusts its clocks over time, whether due to NTP skew, or delays in updating the derived
            // monotonic time, and so on, our original measurement base from the reference source -- which we use to
            // anchor how we convert our scaled source measurement into the same reference timebase -- can skew further
            // away from the current reference time in terms of the rate at which it ticks forward.
            //
            // Essentially, what we're saying here is that we want to test the scaling ratio that we generated in
            // calibration, but not necessarily that the resulting value -- which is meant to be in the same timebase as
            // the reference -- is locked to the reference itself. For example, if the reference is in nanoseconds, we
            // want our source to be scaled to nanoseconds, too. We don't care if the system shoves the reference back
            // and forth via NTP skew, etc... we just need to do enough source-to-reference calibration loops to figure
            // out what the right amount is to scale the TSC -- since we require an invariant/nonstop TSC -- to get it
            // to nanoseconds.
            //
            // At the risk of saying _too much_, while the delta between `Clock::now` and `Monotonic::now` may grow over
            // time if the timebases are not reset, we can readily observe in this test that the delta between the
            // first/last measurement loop for both source/reference are independently close i.e. the ratio by which we
            // scale the source measurements gets it close, and stays close, to the reference measurements in terms of
            // the _passage_ of time.
            clock.reset_timebase();

            for i in 0..1024 {
                src_samples[i] = clock.now().0;
                ref_samples[i] = reference.now();
            }

            let is_src_monotonic = src_samples
                .iter()
                .map(Some)
                .reduce(|last, current| last.and_then(|lv| current.filter(|cv| *cv >= lv)))
                .flatten()
                .copied();
            assert_eq!(is_src_monotonic, Some(src_samples[1023]));

            let is_ref_monotonic = ref_samples
                .iter()
                .map(Some)
                .reduce(|last, current| last.and_then(|lv| current.filter(|cv| *cv >= lv)))
                .flatten()
                .copied();
            assert_eq!(is_ref_monotonic, Some(ref_samples[1023]));

            let local = src_samples
                .iter()
                .zip(ref_samples.iter())
                .map(|(s, r)| *s as f64 - *r as f64)
                .map(|f| f.abs())
                .collect::<Variance>();

            overall.merge(&local);
        }

        println!(
            "reference/source delta: mean={} error={} mean-var={} samples={}",
            overall.mean(),
            overall.error(),
            overall.variance_of_mean(),
            overall.len(),
        );

        // If things are out of sync more than 1000ns, something is likely scaled wrong.
        assert!(overall.mean() < 1000.0);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    #[cfg_attr(not(feature = "flaky_tests"), ignore)]
    fn measure_source_reference_self_timing() {
        let source = Counter::default();
        let reference = Monotonic::default();

        let loops = 10000;

        let mut src_deltas = Vec::new();
        let mut src_samples = [0u64; 100];

        for _ in 0..loops {
            let start = Instant::now();
            for i in 0..100 {
                src_samples[i] = source.now();
            }

            src_deltas.push(start.elapsed().as_secs_f64());
        }

        let mut ref_deltas = Vec::new();
        let mut ref_samples = [0u64; 100];

        for _ in 0..loops {
            let start = Instant::now();
            for i in 0..100 {
                ref_samples[i] = reference.now();
            }

            ref_deltas.push(start.elapsed().as_secs_f64());
        }

        let src_variance = src_deltas.into_iter().collect::<Variance>();
        let ref_variance = ref_deltas.into_iter().collect::<Variance>();

        let src_variance_ns = Duration::from_secs_f64(src_variance.mean() / 100.0);
        let ref_variance_ns = Duration::from_secs_f64(ref_variance.mean() / 100.0);

        println!(
            "source call average: {:?}, reference call average: {:?}",
            src_variance_ns, ref_variance_ns
        );
    }
}
