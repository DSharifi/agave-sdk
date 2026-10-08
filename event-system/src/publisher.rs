use {
    crate::{Event, backend},
    std::fmt::Debug,
};

/// Publishes events of a specific type to a stream.
pub struct Publisher<E: Event> {
    inner: Backend<E>,
}

enum Backend<E: Event> {
    Platform(backend::Publisher<E>),
    Stub(backend::stub::Publisher<E>),
}

impl<E: Event> Publisher<E> {
    /// Creates a no-op publisher on any platform without accessing the filesystem.
    ///
    /// Published events are discarded without serialization.
    pub fn stub() -> Self {
        Self::from_stub(backend::stub::Publisher::new())
    }

    pub fn publish(&mut self, event: &E) -> Result<(), PublishError> {
        match &mut self.inner {
            Backend::Platform(inner) => inner.publish(event),
            Backend::Stub(inner) => inner.publish(event),
        }
    }

    /// Returns whether the stream policy currently enables this publisher's stream.
    ///
    /// Checking this first lets callers skip building events that would be discarded
    /// by [`Publisher::publish`] and  [`Publisher::publish_batch`].
    pub fn is_enabled(&mut self) -> bool {
        match &mut self.inner {
            Backend::Platform(inner) => inner.is_enabled(),
            Backend::Stub(inner) => inner.is_enabled(),
        }
    }

    /// Publishes the given batch of events on the stream.
    ///
    /// # Errors
    /// If any event in the batch fails to send, [`PublishError`] is returned
    /// and the remaining events in the batch are dropped.
    ///
    /// The events previous to the failing event are all sent.
    pub fn publish_batch(&mut self, events: &[E]) -> Result<(), PublishError> {
        match &mut self.inner {
            Backend::Platform(inner) => inner.publish_batch(events),
            Backend::Stub(inner) => inner.publish_batch(events),
        }
    }

    pub(crate) fn new(inner: backend::Publisher<E>) -> Self {
        Self {
            inner: Backend::Platform(inner),
        }
    }
    pub(crate) fn from_stub(inner: backend::stub::Publisher<E>) -> Self {
        Self {
            inner: Backend::Stub(inner),
        }
    }
}

impl<E: Event> Debug for Publisher<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            Backend::Platform(inner) => Debug::fmt(inner, f),
            Backend::Stub(inner) => Debug::fmt(inner, f),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum PublishError {
    #[error("Failed to serialize the event")]
    Serialization(wincode::WriteError),
    #[error("Failed to send the event. Back-pressured by event subscribers.")]
    FailedToSend,
}
