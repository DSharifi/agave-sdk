use {
    crate::Event,
    shaq::broadcast::Broadcast,
    std::{fs::File, sync::Arc},
};

/// A handle to a typed event stream.
pub struct EventHandle<E: Event> {
    pub(crate) broadcast: Broadcast<E::QueueCell>,
    pub(crate) queue_file: Arc<File>,
}

impl<E: Event> EventHandle<E> {
    pub(crate) fn new(broadcast: Broadcast<E::QueueCell>, queue_file: Arc<File>) -> Self {
        Self {
            broadcast,
            queue_file,
        }
    }
}

impl<E: Event> Clone for EventHandle<E> {
    fn clone(&self) -> Self {
        Self {
            broadcast: self.broadcast.clone(),
            queue_file: Arc::clone(&self.queue_file),
        }
    }
}

impl<E: Event> std::fmt::Debug for EventHandle<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EventHandle")
            .field("broadcast", &self.broadcast)
            .finish()
    }
}
