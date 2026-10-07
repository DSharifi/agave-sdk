use {
    super::EventStream,
    crate::{Event, publisher::PublishError},
    shaq::broadcast::{Producer, ProducerId},
    std::{
        fmt::Debug,
        num::NonZeroUsize,
        sync::{Arc, atomic::Ordering},
    },
};

/// Publishes events of a specific type to a stream.
pub(crate) struct Publisher<E: Event> {
    stream: Arc<EventStream<E>>,
    producer_id: ProducerId,
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

        // SAFETY: write_guard is initialized below before it is dropped by going out of scope.
        let mut write_guard =
            unsafe { producer.try_reserve_write() }.ok_or(PublishError::FailedToSend)?;

        let write_guard_cell = write_guard.as_mut();
        // SAFETY: the inner cell contains [u8; N] which is valid for every bit pattern.
        let cell = unsafe { write_guard_cell.assume_init_mut() };

        // if serialization fails we still send incomplete bytes, as drop implementation of
        // write_guard does the sending.
        wincode::serialize_into(cell.as_mut(), &event).map_err(PublishError::Serialization)?;

        Ok(())
    }

    /// Publishes the given batch of events.
    ///
    /// # Errors
    /// If any event in the batch fails to send, [`PublishError`] is returned
    /// and the remaining events in the batch are dropped.
    ///
    /// The events previous to the failing event are all sent.
    pub(crate) fn publish_batch(&mut self, events: &[E]) -> Result<(), PublishError> {
        let Ok(event_count) = NonZeroUsize::try_from(events.len()) else {
            // nothing to write
            return Ok(());
        };

        let Some(producer) = self.producer() else {
            // the stream is disabled
            return Ok(());
        };

        // SAFETY: write_guard cells are initialized in the loop below before it is dropped by going out of scope.
        let mut write_guard = unsafe { producer.try_reserve_write_batch(event_count) }
            .ok_or(PublishError::FailedToSend)?;

        for (i, event) in events.iter().enumerate() {
            // SAFETY: i < events.len() which is the batch size
            let write_guard_cell = unsafe { write_guard.as_mut(i) };
            // SAFETY: the inner cell contains [u8; N] which is valid for every bit pattern.
            let cell = unsafe { write_guard_cell.assume_init_mut() };

            // if serialization fails we still send incomplete bytes, as drop implementation of
            // write_guard does the sending.
            wincode::serialize_into(cell.as_mut(), &event).map_err(PublishError::Serialization)?;
        }

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
        self.producer = stream_state.create_producer(self.producer_id);
    }

    pub(crate) fn is_enabled(&mut self) -> bool {
        self.producer().is_some()
    }
}

impl<E: Event> Publisher<E> {
    pub(super) fn new(
        stream: Arc<EventStream<E>>,
        producer_id: ProducerId,
        queue_generation: u64,
        producer: Option<Producer<E::QueueCell>>,
    ) -> Self {
        Self {
            stream,
            producer_id,
            queue_generation,
            producer,
        }
    }
}

impl<E: Event> Debug for Publisher<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Publisher")
            .field("producer_id", &self.producer_id)
            .field("producer", &self.producer)
            .finish_non_exhaustive()
    }
}
