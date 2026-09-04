use {
    crate::{
        Event,
        event_system::{CreateEventHandleError, CreateEventSystemError, EventStreamConfig},
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

    pub(crate) fn create_event_handle<E: Event>(
        &self,
        _event_stream_name: &str,
        _event_stream_config: EventStreamConfig,
    ) -> Result<EventHandle<E>, CreateEventHandleError> {
        Ok(EventHandle::new())
    }
}

pub(crate) struct EventHandle<E: Event> {
    _queue_cell: PhantomData<E::QueueCell>,
}

impl<E: Event> EventHandle<E> {
    fn new() -> Self {
        Self {
            _queue_cell: PhantomData,
        }
    }
}

impl<E: Event> Clone for EventHandle<E> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<E: Event> std::fmt::Debug for EventHandle<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("EventHandle").finish()
    }
}
