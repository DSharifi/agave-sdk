use {
    super::{EventProducer, EventStream},
    crate::{
        Event,
        event_message::{self, ThreadId},
        publisher::PublishError,
    },
    std::{
        fmt::Debug,
        num::NonZeroUsize,
        sync::{Arc, atomic::Ordering},
    },
};

thread_local! {
    /// The cached id of the current thread to avoid syscalls
    static THREAD_ID: ThreadId = {
        let thread_id: i32 = nix::unistd::gettid().as_raw();
        ThreadId::try_from(thread_id)
            .expect("gettid man page: `call is always sucessful`, meaning a positive i32 is returned")
    }
}

/// Publishes events of a specific type to a stream.
pub(crate) struct Publisher<E: Event> {
    stream: Arc<EventStream<E>>,
    /// The generation of the stream's queue that `producer` belongs to.
    queue_generation: u64,
    /// The producer on the stream's queue, which is `None` while the stream is disabled.
    producer: Option<EventProducer<E>>,
}

impl<E: Event> Publisher<E> {
    pub(crate) fn publish(&mut self, event: &E) -> Result<(), PublishError> {
        let Some(producer) = self.producer() else {
            // the stream is disabled
            return Ok(());
        };
        let thread_id = THREAD_ID.with(|thread_id| *thread_id);

        // SAFETY: write_guard is initialized below before it is dropped by going out of scope.
        let mut write_guard =
            unsafe { producer.try_reserve_write() }.ok_or(PublishError::FailedToSend)?;

        let write_guard_cell = write_guard.as_mut();
        // SAFETY: `MessageCell` is a padding-free `repr(C)` struct of byte arrays (the header and a
        // sealed `ByteArray`), so every bit pattern is a valid value.
        let message_cell: &mut event_message::MessageCell<<E as Event>::QueueCell> =
            unsafe { write_guard_cell.assume_init_mut() };

        // if serialization fails we still send incomplete bytes, as drop implementation of
        // write_guard does the sending.
        message_cell
            .write(thread_id, event)
            .map_err(PublishError::Serialization)?;

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
        let thread_id = THREAD_ID.with(|thread_id| *thread_id);

        // SAFETY: write_guard cells are initialized in the loop below before it is dropped by going out of scope.
        let mut write_guard = unsafe { producer.try_reserve_write_batch(event_count) }
            .ok_or(PublishError::FailedToSend)?;

        for (i, event) in events.iter().enumerate() {
            // SAFETY: i < events.len() which is the batch size
            let write_guard_cell = unsafe { write_guard.as_mut(i) };
            // SAFETY: `MessageCell` is a padding-free `repr(C)` struct of byte arrays (the header and a
            // sealed `ByteArray`), so every bit pattern is a valid value.
            let message_cell: &mut event_message::MessageCell<<E as Event>::QueueCell> =
                unsafe { write_guard_cell.assume_init_mut() };

            // if serialization fails we still send incomplete bytes, as drop implementation of
            // write_guard does the sending.
            message_cell
                .write(thread_id, event)
                .map_err(PublishError::Serialization)?;
        }

        Ok(())
    }

    /// Returns the producer on the stream's current queue, or `None` while the
    /// stream is disabled.
    #[inline]
    fn producer(&mut self) -> Option<&mut EventProducer<E>> {
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
}

impl<E: Event> Publisher<E> {
    pub(super) fn new(
        stream: Arc<EventStream<E>>,
        queue_generation: u64,
        producer: Option<EventProducer<E>>,
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
