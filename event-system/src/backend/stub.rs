use {
    crate::{
        Event,
        event_system::{CreateEventSystemError, CreateStreamError, StreamConfig},
        producer::EmitEventError,
    },
    std::{marker::PhantomData, path::Path},
};

pub(crate) struct Producer<E> {
    _data: PhantomData<E>,
}

impl<E> Producer<E> {
    pub(crate) fn emit_event(&mut self, _event: &E) -> Result<(), EmitEventError> {
        Ok(())
    }
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("infallible error case for stub implementation.")]
pub(crate) struct EventQueueError;

#[derive(Debug, Clone)]
pub(crate) struct EventSystem;

impl EventSystem {
    pub(crate) fn new(
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

    pub(crate) fn try_create_producer(&self) -> Option<Producer<E>> {
        Some(Producer { _data: PhantomData })
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
