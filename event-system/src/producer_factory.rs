use crate::{Event, backend};

/// A handle to a typed [`Event`] stream that can create producers
/// on demand.
pub struct ProducerFactory<E: Event> {
    backend: backend::ProducerFactory<E>,
}

impl<E: Event> ProducerFactory<E> {
    pub(crate) fn new(backend: backend::ProducerFactory<E>) -> Self {
        Self { backend }
    }
}

impl<E: Event> Clone for ProducerFactory<E> {
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
        }
    }
}

impl<E: Event> std::fmt::Debug for ProducerFactory<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.backend.fmt(formatter)
    }
}
