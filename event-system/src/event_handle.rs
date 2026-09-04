use crate::{Event, backend};

/// A handle to a typed event stream.
pub struct EventHandle<E: Event> {
    backend: backend::EventHandle<E>,
}

impl<E: Event> EventHandle<E> {
    pub(crate) fn new(backend: backend::EventHandle<E>) -> Self {
        Self { backend }
    }
}

impl<E: Event> Clone for EventHandle<E> {
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
        }
    }
}

impl<E: Event> std::fmt::Debug for EventHandle<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.backend.fmt(formatter)
    }
}
