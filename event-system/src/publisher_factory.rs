use crate::{Event, backend, publisher::Publisher};

/// A handle to a typed [`Event`] stream that can create publishers
/// on demand.
pub struct PublisherFactory<E: Event> {
    backend: backend::PublisherFactory<E>,
}

impl<E: Event> PublisherFactory<E> {
    pub fn try_create_publisher(&self) -> Option<Publisher<E>> {
        self.backend.try_create_publisher().map(Publisher::new)
    }

    pub(crate) fn new(backend: backend::PublisherFactory<E>) -> Self {
        Self { backend }
    }
}

impl<E: Event> Clone for PublisherFactory<E> {
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
        }
    }
}

impl<E: Event> std::fmt::Debug for PublisherFactory<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.backend.fmt(formatter)
    }
}
