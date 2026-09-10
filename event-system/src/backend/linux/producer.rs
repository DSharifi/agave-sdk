use {
    crate::{Event, backend::StreamGuard, producer::EmitEventError},
    std::{fmt::Debug, sync::Arc},
};

/// A producer which can emit events of a specific type.
pub(crate) struct Producer<E: Event> {
    broadcast_sender: shaq::broadcast::Producer<E::QueueCell>,
    stream_guard: Arc<StreamGuard>,
}

impl<E: Event> Producer<E> {
    pub(crate) fn emit_event(&mut self, event: &E) -> Result<(), EmitEventError> {
        // SAFETY: write_guard is initialized below before it is dropped by going out of scope.
        let mut write_guard = unsafe { self.broadcast_sender.try_reserve_write() }
            .ok_or(EmitEventError::FailedToSend)?;

        let write_guard_cell = write_guard.as_mut();
        // SAFETY: the inner cell contains [u8; N] which is valid for every bit pattern.
        let cell = unsafe { write_guard_cell.assume_init_mut() };

        // if serialization fails we still send incomplete bytes, as drop implementation of
        // write_guard does the sending.
        wincode::serialize_into(cell.as_mut(), &event).map_err(EmitEventError::Serialization)?;

        Ok(())
    }
}

impl<E: Event> Producer<E> {
    pub(super) fn new(
        broadcast_sender: shaq::broadcast::Producer<E::QueueCell>,
        stream_guard: Arc<StreamGuard>,
    ) -> Self {
        Self {
            broadcast_sender,
            stream_guard,
        }
    }
}
impl<E: Event> Debug for Producer<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Producer")
            .field("broadcast_sender", &self.broadcast_sender)
            .field("stream_guard", &self.stream_guard)
            .finish()
    }
}
