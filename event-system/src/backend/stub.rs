use {
    crate::{
        Event,
        event_system::{CreateEventSystemError, CreateStreamError, StreamConfig},
    },
    std::{marker::PhantomData, path::Path},
};

#[derive(Debug, Clone, thiserror::Error)]
#[error("infallible error case for stub implementation.")]
pub(crate) struct EventQueueError;

#[derive(Debug, Clone)]
pub(crate) struct EventSystem;

impl EventSystem {
    pub(crate) fn create(
        _event_system_directory: impl AsRef<Path>,
    ) -> Result<Self, CreateEventSystemError> {
        Ok(Self)
    }

    pub(crate) fn create_stream<E: Event>(
        &self,
        _stream_name: &str,
        _stream_config: StreamConfig,
    ) -> Result<ProducerFactory<E>, CreateStreamError> {
        Ok(ProducerFactory::new())
    }
}

pub(crate) struct ProducerFactory<E: Event> {
    _queue_cell: PhantomData<E::QueueCell>,
}

impl<E: Event> ProducerFactory<E> {
    fn new() -> Self {
        Self {
            _queue_cell: PhantomData,
        }
    }
}

impl<E: Event> Clone for ProducerFactory<E> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<E: Event> std::fmt::Debug for ProducerFactory<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("ProducerFactory").finish()
    }
}
