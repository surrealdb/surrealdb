use anyhow::Result;

pub trait PrefixError<T> {
    fn prefix_err<F, S>(self, prefix: F) -> Result<T>
    where
        F: FnOnce() -> S,
        S: std::fmt::Display;
}

impl<T, E> PrefixError<T> for std::result::Result<T, E>
where
    E: std::fmt::Display + Send + Sync + 'static,
{
    fn prefix_err<F, S>(self, prefix: F) -> Result<T>
    where
        F: FnOnce() -> S,
        S: std::fmt::Display,
    {
        self.map_err(|e| anyhow::anyhow!(format!("{}: {}", prefix(), e)))
    }
}

impl<T> PrefixError<T> for Option<T> {
    fn prefix_err<F, S>(self, prefix: F) -> Result<T>
    where
        F: FnOnce() -> S,
        S: std::fmt::Display,
    {
        self.ok_or_else(|| anyhow::anyhow!(format!("{}: None", prefix())))
    }
}
