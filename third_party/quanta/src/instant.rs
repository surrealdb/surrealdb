use std::cmp::{Ord, Ordering, PartialOrd};
use std::fmt;
use std::ops::{Add, AddAssign, Sub, SubAssign};
use std::time::Duration;

/// A point-in-time wall-clock measurement.
///
/// Mimics most of the functionality of [`std::time::Instant`] but provides an additional method for
/// using the "recent time" feature of `quanta`.
///
/// ## Monotonicity
///
/// On all platforms, `Instant` will try to use an OS API that guarantees monotonic behavior if
/// available, which is the case for all supported platforms.  In practice such guarantees are –
/// under rare circumstances – broken by hardware, virtualization or operating system bugs. To work
/// around these bugs and platforms not offering monotonic clocks [`duration_since`], [`elapsed`]
/// and [`sub`] saturate to zero. In older `quanta` versions this lead to a panic instead.
/// [`checked_duration_since`] can be used to detect and handle situations where monotonicity is
/// violated, or `Instant`s are subtracted in the wrong order.
///
/// This workaround obscures programming errors where earlier and later instants are accidentally
/// swapped. For this reason future `quanta` versions may reintroduce panics.
///
/// [`duration_since`]: Instant::duration_since
/// [`elapsed`]: Instant::elapsed
/// [`sub`]: Instant::sub
/// [`checked_duration_since`]: Instant::checked_duration_since
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Instant(pub(crate) u64);

impl Instant {
    /// Gets the current time, scaled to reference time.
    ///
    /// This method depends on a lazily initialized global clock, which can take up to 200ms to
    /// initialize and calibrate itself.
    ///
    /// This method is the spiritual equivalent of [`Instant::now`][instant_now].  It is guaranteed to
    /// return a monotonically increasing value.
    ///
    /// [instant_now]: std::time::Instant::now
    pub fn now() -> Instant {
        crate::get_now()
    }

    /// Gets the most recent current time, scaled to reference time.
    ///
    /// This method provides ultra-low-overhead access to a slightly-delayed version of the current
    /// time.  Instead of querying the underlying source clock directly, a shared, global value is
    /// read directly without the need to scale to reference time.
    ///
    /// The value is updated by running an "upkeep" thread or by calling [`set_recent`][set_recent].  An
    /// upkeep thread can be configured and spawned via [`Upkeep`][upkeep].
    ///
    /// If the upkeep thread has not been started, or no value has been set manually, a lazily
    /// initialized global clock will be used to get the current time.  This clock can take up to
    /// 200ms to initialize and calibrate itself.
    ///
    /// [set_recent]: crate::set_recent
    /// [upkeep]: crate::Upkeep
    pub fn recent() -> Instant {
        crate::get_recent()
    }

    /// Returns the amount of time elapsed from another instant to this one.
    ///
    /// # Panics
    ///
    /// Previous versions of this method panicked when earlier was later than `self`. Currently,
    /// this method saturates to zero. Future versions may reintroduce the panic in some
    /// circumstances. See [Monotonicity].
    ///
    /// [Monotonicity]: Instant#monotonicity
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use quanta::Clock;
    /// use std::time::Duration;
    /// use std::thread::sleep;
    ///
    /// let mut clock = Clock::new();
    /// let now = clock.now();
    /// sleep(Duration::new(1, 0));
    /// let new_now = clock.now();
    /// println!("{:?}", new_now.duration_since(now));
    /// ```
    pub fn duration_since(&self, earlier: Instant) -> Duration {
        self.checked_duration_since(earlier).unwrap_or_default()
    }

    /// Returns the amount of time elapsed from another instant to this one, or `None` if that
    /// instant is earlier than this one.
    ///
    /// Due to [monotonicity bugs], even under correct logical ordering of the passed `Instant`s,
    /// this method can return `None`.
    ///
    /// [monotonicity bugs]: Instant#monotonicity
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use quanta::Clock;
    /// use std::time::Duration;
    /// use std::thread::sleep;
    ///
    /// let mut clock = Clock::new();
    /// let now = clock.now();
    /// sleep(Duration::new(1, 0));
    /// let new_now = clock.now();
    /// println!("{:?}", new_now.checked_duration_since(now));
    /// println!("{:?}", now.checked_duration_since(new_now)); // None
    /// ```
    pub fn checked_duration_since(&self, earlier: Instant) -> Option<Duration> {
        self.0.checked_sub(earlier.0).map(Duration::from_nanos)
    }

    /// Returns the amount of time elapsed from another instant to this one, or zero duration if
    /// that instant is earlier than this one.
    ///
    /// Due to [monotonicity bugs], even under correct logical ordering of the passed `Instant`s,
    /// this method can return `None`.
    ///
    /// [monotonicity bugs]: Instant#monotonicity
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use quanta::Clock;
    /// use std::time::Duration;
    /// use std::thread::sleep;
    ///
    /// let mut clock = Clock::new();
    /// let now = clock.now();
    /// sleep(Duration::new(1, 0));
    /// let new_now = clock.now();
    /// println!("{:?}", new_now.saturating_duration_since(now));
    /// println!("{:?}", now.saturating_duration_since(new_now)); // 0ns
    /// ```
    pub fn saturating_duration_since(&self, earlier: Instant) -> Duration {
        self.checked_duration_since(earlier).unwrap_or_default()
    }

    /// Returns the amount of time elapsed since this instant was created.
    ///
    /// # Panics
    ///
    /// Previous `quanta` versions panicked when the current time was earlier than self.  Currently
    /// this method returns a Duration of zero in that case.  Future versions may reintroduce the
    /// panic. See [Monotonicity].
    ///
    /// [Monotonicity]: Instant#monotonicity
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use quanta::Clock;
    /// use std::time::Duration;
    /// use std::thread::sleep;
    ///
    /// let mut clock = Clock::new();
    /// let now = clock.now();
    /// sleep(Duration::new(1, 0));
    /// let elapsed = now.elapsed();
    /// println!("{:?}", elapsed); // ≥ 1s
    /// ```
    pub fn elapsed(&self) -> Duration {
        Instant::now() - *self
    }

    /// Returns `Some(t)` where `t` is the time `self + duration` if `t` can be represented as
    /// `Instant` (which means it's inside the bounds of the underlying data structure), `None`
    /// otherwise.
    pub fn checked_add(&self, duration: Duration) -> Option<Instant> {
        let total_nanos = u64::try_from(duration.as_nanos()).ok()?;
        self.0.checked_add(total_nanos).map(Instant)
    }

    /// Returns `Some(t)` where `t` is the time `self - duration` if `t` can be represented as
    /// `Instant` (which means it's inside the bounds of the underlying data structure), `None`
    /// otherwise.
    pub fn checked_sub(&self, duration: Duration) -> Option<Instant> {
        let total_nanos = u64::try_from(duration.as_nanos()).ok()?;
        self.0.checked_sub(total_nanos).map(Instant)
    }
}

impl Add<Duration> for Instant {
    type Output = Instant;

    /// # Panics
    ///
    /// This function may panic if the resulting point in time cannot be represented by the
    /// underlying data structure. See [`Instant::checked_add`] for a version without panic.
    fn add(self, other: Duration) -> Instant {
        self.checked_add(other)
            .expect("overflow when adding duration to instant")
    }
}

impl AddAssign<Duration> for Instant {
    fn add_assign(&mut self, other: Duration) {
        // This is not millenium-safe, but, I think that's OK. :)
        self.0 = self.0 + other.as_nanos() as u64;
    }
}

impl Sub<Duration> for Instant {
    type Output = Instant;

    fn sub(self, other: Duration) -> Instant {
        self.checked_sub(other)
            .expect("overflow when subtracting duration from instant")
    }
}

impl SubAssign<Duration> for Instant {
    fn sub_assign(&mut self, other: Duration) {
        // This is not millenium-safe, but, I think that's OK. :)
        self.0 = self.0 - other.as_nanos() as u64;
    }
}

impl Sub<Instant> for Instant {
    type Output = Duration;

    /// Returns the amount of time elapsed from another instant to this one,
    /// or zero duration if that instant is later than this one.
    ///
    /// # Panics
    ///
    /// Previous `quanta` versions panicked when `other` was later than `self`. Currently this
    /// method saturates. Future versions may reintroduce the panic in some circumstances.
    /// See [Monotonicity].
    ///
    /// [Monotonicity]: Instant#monotonicity
    fn sub(self, other: Instant) -> Duration {
        self.duration_since(other)
    }
}

impl PartialOrd for Instant {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Instant {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl fmt::Debug for Instant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(feature = "prost")]
impl Into<prost_types::Timestamp> for Instant {
    fn into(self) -> prost_types::Timestamp {
        let dur = Duration::from_nanos(self.0);
        let secs = if dur.as_secs() > i64::MAX as u64 {
            i64::MAX
        } else {
            dur.as_secs() as i64
        };
        let nsecs = if dur.subsec_nanos() > i32::MAX as u32 {
            i32::MAX
        } else {
            dur.subsec_nanos() as i32
        };
        prost_types::Timestamp {
            seconds: secs,
            nanos: nsecs,
        }
    }
}

#[cfg(test)]
mod tests {
    use once_cell::sync::Lazy;

    use super::Instant;
    use crate::{with_clock, Clock};
    use std::time::Duration;
    use std::{sync::Mutex, thread};

    static RECENT_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

    #[test]
    #[cfg_attr(
        all(target_arch = "wasm32", target_os = "unknown"),
        ignore = "WASM thread cannot sleep"
    )]
    fn test_now() {
        let t0 = Instant::now();
        thread::sleep(Duration::from_millis(15));
        let t1 = Instant::now();

        assert!(t0.0 > 0);
        assert!(t1.0 > 0);

        let result = t1 - t0;
        let threshold = Duration::from_millis(14);
        assert!(result > threshold);
    }

    #[test]
    #[cfg_attr(
        all(target_arch = "wasm32", target_os = "unknown"),
        ignore = "WASM thread cannot sleep"
    )]
    fn test_recent() {
        let _guard = RECENT_LOCK.lock().unwrap();

        // Ensures that the recent global value is zero so that the fallback logic can kick in.
        crate::set_recent(Instant(0));

        let t0 = Instant::recent();
        thread::sleep(Duration::from_millis(15));
        let t1 = Instant::recent();

        assert!(t0.0 > 0);
        assert!(t1.0 > 0);

        let result = t1 - t0;
        let threshold = Duration::from_millis(14);
        assert!(
            result > threshold,
            "t1 should be greater than t0 by at least 14ms, was only {:?} (t0: {}, t1: {})",
            result,
            t0.0,
            t1.0
        );

        crate::set_recent(Instant(1));
        let t2 = Instant::recent();
        thread::sleep(Duration::from_millis(15));
        let t3 = Instant::recent();
        assert_eq!(t2, t3);
    }

    #[test]
    #[cfg_attr(
        all(target_arch = "wasm32", target_os = "unknown"),
        wasm_bindgen_test::wasm_bindgen_test
    )]
    fn test_mocking() {
        let _guard = RECENT_LOCK.lock().unwrap();

        // Ensures that the recent global value is zero so that the fallback logic can kick in.
        crate::set_recent(Instant(0));

        let (clock, mock) = Clock::mock();
        with_clock(&clock, move || {
            let t0 = Instant::now();
            mock.increment(42);
            let t1 = Instant::now();

            assert_eq!(t0.0, 0);
            assert_eq!(t1.0, 42);

            let t2 = Instant::recent();
            mock.increment(420);
            let t3 = Instant::recent();

            assert_eq!(t2.0, 42);
            assert_eq!(t3.0, 462);

            crate::set_recent(Instant(1440));
            let t4 = Instant::recent();
            assert_eq!(t4.0, 1440);
        })
    }

    // Test fix for issue #109
    #[test]
    #[cfg_attr(
        all(target_arch = "wasm32", target_os = "unknown"),
        wasm_bindgen_test::wasm_bindgen_test
    )]
    fn checked_arithmetic_u64_overflow() {
        fn nanos_to_dur(total_nanos: u128) -> Duration {
            let nanos_per_sec = Duration::from_secs(1).as_nanos();
            let secs = total_nanos / nanos_per_sec;
            let nanos = total_nanos % nanos_per_sec;
            let dur = Duration::new(secs as _, nanos as _);
            assert_eq!(dur.as_nanos(), total_nanos);
            dur
        }

        let dur = nanos_to_dur(1 << 64);
        let now = Instant::now();

        let behind = now.checked_sub(dur);
        let ahead = now.checked_add(dur);

        assert_ne!(Duration::ZERO, dur);
        assert_ne!(Some(now), behind);
        assert_ne!(Some(now), ahead);
    }
}
