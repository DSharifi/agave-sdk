use {
    super::EventStream,
    crate::{Event, publisher::PublishError},
    shaq::broadcast::Producer,
    std::{
        fmt::Debug,
        num::NonZeroUsize,
        sync::{Arc, atomic::Ordering},
    },
};

/// Publishes events of a specific type to a stream.
pub(crate) struct Publisher<E: Event> {
    stream: Arc<EventStream<E>>,
    /// The generation of the stream's queue that `producer` belongs to.
    queue_generation: u64,
    /// The producer on the stream's queue, which is `None` while the stream is disabled.
    producer: Option<Producer<E::QueueCell>>,
}

impl<E: Event> Publisher<E> {
    pub(crate) fn publish(&mut self, event: &E) -> Result<(), PublishError> {
        let Some(producer) = self.producer() else {
            // the stream is disabled
            return Ok(());
        };

        // SAFETY: write_guard is initialized below before it is published
        let mut write_guard =
            unsafe { producer.try_reserve_write() }.ok_or(PublishError::FailedToSend)?;

        let write_guard_cell = write_guard.as_mut();
        // SAFETY: the inner cell contains [u8; N] which is valid for every bit pattern.
        let cell = unsafe { write_guard_cell.assume_init_mut() };

        // if serialization fails, write_guard is dropped without publishing, so nothing is sent.
        wincode::serialize_into(cell.as_mut(), &event).map_err(PublishError::Serialization)?;
        write_guard.publish();

        Ok(())
    }

    /// Publishes the given batch of events.
    ///
    /// # Errors
    /// If the batch fails to send or any event in it fails to serialize,
    /// [`PublishError`] is returned and none of the events in the batch are sent.
    pub(crate) fn publish_batch(&mut self, events: &[E]) -> Result<(), PublishError> {
        let Ok(event_count) = NonZeroUsize::try_from(events.len()) else {
            // nothing to write
            return Ok(());
        };

        let Some(producer) = self.producer() else {
            // the stream is disabled
            return Ok(());
        };

        // SAFETY: write_guard cells are initialized in the loop below before it is published
        let mut write_guard = unsafe { producer.try_reserve_write_batch(event_count) }
            .ok_or(PublishError::FailedToSend)?;

        for (i, event) in events.iter().enumerate() {
            // SAFETY: i < events.len() which is the batch size
            let write_guard_cell = unsafe { write_guard.as_mut(i) };
            // SAFETY: the inner cell contains [u8; N] which is valid for every bit pattern.
            let cell = unsafe { write_guard_cell.assume_init_mut() };

            // if serialization fails, write_guard is dropped without publishing, so nothing in
            // the batch is sent.
            wincode::serialize_into(cell.as_mut(), &event).map_err(PublishError::Serialization)?;
        }
        write_guard.publish();

        Ok(())
    }

    /// Returns the producer on the stream's current queue, or `None` while the
    /// stream is disabled.
    #[inline]
    fn producer(&mut self) -> Option<&mut Producer<E::QueueCell>> {
        if self.stream.queue_generation.load(Ordering::Relaxed) != self.queue_generation {
            self.replace_producer();
        }

        self.producer.as_mut()
    }

    /// Replaces the producer on the stream's previous queue with a producer on
    /// its current queue.
    #[cold]
    fn replace_producer(&mut self) {
        // releases the previous queue
        self.producer = None;

        let stream_state = self.stream.state.read().unwrap();
        self.queue_generation = self.stream.queue_generation.load(Ordering::Relaxed);
        self.producer = stream_state.create_producer();
    }

    pub(crate) fn is_enabled(&mut self) -> bool {
        self.producer().is_some()
    }
}

impl<E: Event> Publisher<E> {
    pub(super) fn new(
        stream: Arc<EventStream<E>>,
        queue_generation: u64,
        producer: Option<Producer<E::QueueCell>>,
    ) -> Self {
        Self {
            stream,
            queue_generation,
            producer,
        }
    }
}

impl<E: Event> Debug for Publisher<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Publisher")
            .field("producer", &self.producer)
            .finish_non_exhaustive()
    }
}
