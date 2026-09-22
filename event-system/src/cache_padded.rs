use core::{fmt, ops::Deref};

#[repr(C, align(64))]
pub(crate) struct CachePadded<T> {
    value: T,
}

impl<T> CachePadded<T> {
    pub(crate) const fn new(t: T) -> Self {
        Self { value: t }
    }
}

impl<T> Deref for CachePadded<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T: fmt::Debug> fmt::Debug for CachePadded<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachePadded")
            .field("value", &self.value)
            .finish()
    }
}
