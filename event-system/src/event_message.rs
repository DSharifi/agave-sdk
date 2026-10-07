//! Layout of a message in a queue cell: a little-endian `u32` thread id of the
//! publisher, followed by the wincode-encoded event.

pub(crate) type ThreadId = u32;

/// Size of the header that precedes the encoded event in every queue cell.
pub(crate) const MESSAGE_HEADER_SIZE: usize = size_of::<ThreadId>();

/// The element of a stream's queue: the message header followed by the event's
/// [`QueueCell`](crate::Event::QueueCell).
#[cfg(target_os = "linux")]
#[repr(C)]
#[derive(Clone, Copy)]
pub(crate) struct MessageCell<C> {
    header: [u8; MESSAGE_HEADER_SIZE],
    event: C,
}

#[cfg(target_os = "linux")]
impl<C> MessageCell<C> {
    const HAS_NO_PADDING: bool = {
        let no_padding_after_header = std::mem::offset_of!(Self, event) == MESSAGE_HEADER_SIZE;
        let unpadded_size = MESSAGE_HEADER_SIZE.checked_add(size_of::<C>()).unwrap();
        let no_padding_after_event = unpadded_size == size_of::<Self>();

        no_padding_after_header && no_padding_after_event
    };
}

/// compile time assertions that MessageCell has no padding
const _: () = assert!(MessageCell::<[u8; 0]>::HAS_NO_PADDING);
const _: () = assert!(MessageCell::<[u8; 1]>::HAS_NO_PADDING);
const _: () = assert!(MessageCell::<[u8; 13]>::HAS_NO_PADDING);
const _: () = assert!(!MessageCell::<[u64; 1]>::HAS_NO_PADDING);

#[cfg(target_os = "linux")]
impl<C: crate::ByteArray> MessageCell<C> {
    /// Encodes the message directly into the cell, without an intermediate buffer.
    #[inline]
    pub(crate) fn write<E: crate::Event<QueueCell = C>>(
        &mut self,
        thread_id: ThreadId,
        event: &E,
    ) -> Result<(), wincode::WriteError> {
        const { assert!(Self::HAS_NO_PADDING, "MessageCell must not contain padding") };

        self.header = thread_id.to_le_bytes();
        wincode::serialize_into(self.event.as_mut(), event)
    }
}

/// A message read from a queue cell.
#[cfg(target_os = "linux")]
#[derive(Clone, Copy)]
pub(crate) struct RawMessage<'a> {
    header: &'a [u8; MESSAGE_HEADER_SIZE],
    event: &'a [u8],
}

#[cfg(target_os = "linux")]
impl<'a> RawMessage<'a> {
    /// Returns `None` if `message` is too short to contain the header.
    #[inline]
    pub(crate) fn new(message: &'a [u8]) -> Option<Self> {
        let (header, event) = message.split_first_chunk::<MESSAGE_HEADER_SIZE>()?;
        Some(Self { header, event })
    }

    /// The thread id of the publisher of the message.
    #[inline]
    pub(crate) fn thread_id(&self) -> ThreadId {
        ThreadId::from_le_bytes(*self.header)
    }

    /// The encoded event of the message.
    #[inline]
    pub(crate) fn event(&self) -> &'a [u8] {
        self.event
    }
}
