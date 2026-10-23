#[derive(Clone, Copy, Debug, Default)]
pub struct Monotonic {
    _default: (),
}

impl Monotonic {
    pub fn now(&self) -> u64 {
        unsafe { wasi::clock_time_get(wasi::CLOCKID_MONOTONIC, 1).expect("failed to get time") }
    }
}
