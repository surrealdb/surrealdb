use crate::{set_recent, Clock};
use std::{
    fmt, io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

static GLOBAL_UPKEEP_RUNNING: AtomicBool = AtomicBool::new(false);

/// Ultra-low-overhead access to slightly-delayed time.
///
/// In some applications, there can be a need to check the current time very often, so much so that
/// the overhead of checking the time can begin to eat up measurable overhead. For some of these
/// cases, the time may need to be accessed often but does not necessarily need to be incredibly
/// accurate: one millisecond granularity could be entirely acceptable.
///
/// For these cases, we provide a slightly-delayed version of the time to callers via
/// [`Clock::recent`], which is updated by a background upkeep thread.  That thread is configured
/// and spanwed via [`Upkeep`].
///
/// [`Upkeep`] can construct a new clock (or be passed an existing clock to use), and given an
/// update interval, and it will faithfully attempt to update the global recent time on the
/// specified interval.  There is a trade-off to be struck in terms of how often the time is
/// updated versus the required accuracy.  Checking the time and updating the global reference is
/// itself not zero-cost, and so care must be taken to analyze the number of readers in order to
/// ensure that, given a particular update interval, the upkeep thread is saving more CPU time than
/// would be spent otherwise by directly querying the current time.
///
/// The recent time is read and written atomically.  It is global to an application, so if another
/// codepath creates the upkeep thread, the interval chosen by that instantiation will be the one
/// that all callers of [`Clock::recent`] end up using.
///
/// Multiple upkeep threads cannot exist at the same time.  A new upkeep thread can be started if
/// the old one is dropped and returns.
///
/// In terms of performance, reading the recent time can be up to two to three times as fast as
/// reading the current time in the optimized case of using the Time Stamp Counter source.  In
/// practice, while a caller might expect to take 12-14ns to read the TSC and scale it to reference
/// time, the recent time can be read in 4-5ns, with no reference scale conversion required.
#[derive(Debug)]
pub struct Upkeep {
    interval: Duration,
    clock: Clock,
}

/// Handle to a running upkeep thread.
///
/// If a handle is dropped, the upkeep thread will be stopped, and the recent time will cease to
/// update.  The upkeep thread can be started again to resume updating the recent time.
#[derive(Debug)]
pub struct Handle {
    done: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

/// Errors thrown during the creation/spawning of the upkeep thread.
#[derive(Debug)]
pub enum Error {
    /// An upkeep thread is already running in this process.
    UpkeepRunning,
    /// An error occurred when trying to spawn the upkeep thread.
    FailedToSpawnUpkeepThread(io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::UpkeepRunning => write!(f, "upkeep thread already running"),
            Error::FailedToSpawnUpkeepThread(e) => {
                write!(f, "failed to spawn upkeep thread: {}", e)
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::UpkeepRunning => None,
            Self::FailedToSpawnUpkeepThread(e) => Some(e),
        }
    }
}

impl Upkeep {
    /// Creates a new [`Upkeep`].
    ///
    /// This creates a new internal clock for acquiring the current time.  If you have an existing
    /// [`Clock`] that is already calibrated, it is slightly faster to clone it and construct the
    /// builder with [`new_with_clock`](Upkeep::new_with_clock) to avoid recalibrating.
    pub fn new(interval: Duration) -> Upkeep {
        Self::new_with_clock(interval, Clock::new())
    }

    /// Creates a new [`Upkeep`] with the specified [`Clock`] instance.
    pub fn new_with_clock(interval: Duration, clock: Clock) -> Upkeep {
        Upkeep { interval, clock }
    }

    /// Start the upkeep thread, periodically updating the global coarse time.
    ///
    /// [`Handle`] represents a drop guard for the upkeep thread if it is successfully spawned.
    /// Dropping the handle will also instruct the upkeep thread to stop and exist, so the handle
    /// must be held while the upkeep thread should continue to run.
    ///
    /// # Errors
    ///
    /// If either an existing upkeep thread is running, or there was an issue when attempting to
    /// spawn the upkeep thread, an error variant will be returned describing the error.
    pub fn start(self) -> Result<Handle, Error> {
        // If another upkeep thread is running, inform the caller.
        let _ = GLOBAL_UPKEEP_RUNNING
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .map_err(|_| Error::UpkeepRunning)?;

        let interval = self.interval;
        let clock = self.clock;

        // Note: spawning `quanta-upkeep` thread may take a significant amount of time. Thus, it is
        // possible for a user to read a 0 value from `Clock::recent` before `quanta-upkeep` has
        // started. To avoid that, make sure the recent time is initialized to some measurement.
        set_recent(clock.now());

        let done = Arc::new(AtomicBool::new(false));
        let their_done = done.clone();

        let result = thread::Builder::new()
            .name("quanta-upkeep".to_string())
            .spawn(move || {
                while !their_done.load(Ordering::Acquire) {
                    set_recent(clock.now());

                    thread::sleep(interval);
                }

                GLOBAL_UPKEEP_RUNNING.store(false, Ordering::SeqCst);
            })
            .map_err(Error::FailedToSpawnUpkeepThread);

        // Let another caller attempt to spawn the upkeep thread if we failed to do so.
        if result.is_err() {
            GLOBAL_UPKEEP_RUNNING.store(false, Ordering::SeqCst);
        }

        let handle = result?;

        Ok(Handle {
            done,
            handle: Some(handle),
        })
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.done.store(true, Ordering::Release);

        if let Some(handle) = self.handle.take() {
            let _result = handle
                .join()
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "failed to stop upkeep thread"));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Upkeep;
    use std::time::Duration;

    #[test]
    #[cfg_attr(target_arch = "wasm32", ignore)] // WASM is single threaded
    fn test_spawning_second_upkeep() {
        let first = Upkeep::new(Duration::from_millis(250)).start();
        let second = Upkeep::new(Duration::from_millis(250))
            .start()
            .map_err(|e| e.to_string());

        assert!(first.is_ok());

        let second_err = second.expect_err("second upkeep should be error, got handle");
        assert_eq!(second_err, "upkeep thread already running");
    }
}
