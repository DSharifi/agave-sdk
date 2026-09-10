use {
    crate::{Event, backend},
    std::{marker::PhantomData, rc::Rc},
};

/// A producer which can emit events of a specific type.
///
/// [`Producer<T>`] is [`!Send`](Send) + [`!Sync`](Sync), as a producer is associated
/// with a thread for its entire lifetime.
pub struct Producer<E: Event> {
    inner: backend::Producer<E>,
    //  `Rc` is !Send + !Sync, which makes Producer<E> also neither
    _not_send_or_sync: PhantomData<Rc<()>>,
}

impl<E: Event> Producer<E> {
    pub fn emit_event(&mut self, event: &E) -> Result<(), EmitEventError> {
        self.inner.emit_event(event)
    }

    pub(crate) fn new(inner: backend::Producer<E>) -> Self {
        Self {
            inner,
            _not_send_or_sync: PhantomData,
        }
    }
}

pub enum EmitEventError {}
