use wincode_dynamic::SerializedSize;

/// This ByteArray trait is a workaround as [`generic-const-exprs`](https://doc.rust-lang.org/beta/unstable-book/language-features/generic-const-exprs.html)
/// is not yet stable.
///
/// The trait is sealed, so only `[u8; N]` can be used as a queue cell:
///
/// ```compile_fail
/// # use agave_event_system::{
/// #     Event,
/// #     wincode::{SchemaRead, SchemaWrite},
/// #     wincode_dynamic::SchemaDynamic,
/// # };
/// #[derive(SchemaRead, SchemaWrite, SchemaDynamic)]
/// #[wincode(crate = "agave_event_system::__private::event_macro")]
/// struct Message {
///     value: u64,
/// }
///
/// impl Event for Message {
///     type QueueCell = [u64; 1];
/// }
/// ```
pub trait ByteArray: sealed::Sealed + Copy + Send + Sync + AsMut<[u8]> + 'static {}

impl<const N: usize> ByteArray for [u8; N] {}

/// internal module such that Sealed is only implemented for `[u8; N]` to keep ByteArray's contract
mod sealed {
    pub trait Sealed {}

    impl<const N: usize> Sealed for [u8; N] {}
}

// The function is hidden from docs as it's only intended for macro expansion.
#[doc(hidden)]
pub const fn event_queue_cell_size(
    size: SerializedSize,
    max_serialized_size: Option<usize>,
) -> usize {
    match size {
        SerializedSize::Static(size) => size,
        SerializedSize::Dynamic(lower_bound) => match max_serialized_size {
            Some(max_serialized_size) if max_serialized_size > lower_bound => max_serialized_size,
            Some(_) => {
                panic!(
                    "`max_serialized_size` must be greater than wincode's dynamic serialized-size \
                     lower bound"
                )
            }
            None => {
                panic!(
                    "event has a dynamic serialized size; specify `max_serialized_size` in the \
                     `#[event(...)]` attribute"
                )
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use {super::event_queue_cell_size, wincode_dynamic::SerializedSize};

    #[test]
    fn static_size_uses_wincode_upper_bound() {
        assert_eq!(event_queue_cell_size(SerializedSize::Static(8), None), 8);
        assert_eq!(
            event_queue_cell_size(SerializedSize::Static(8), Some(16)),
            8
        );
    }

    #[test]
    fn dynamic_size_uses_explicit_upper_bound() {
        assert_eq!(
            event_queue_cell_size(SerializedSize::Dynamic(8), Some(16)),
            16
        );
    }

    #[test]
    #[should_panic(expected = "event has a dynamic serialized size")]
    fn dynamic_size_requires_explicit_upper_bound() {
        event_queue_cell_size(SerializedSize::Dynamic(8), None);
    }

    #[test]
    #[should_panic(expected = "must be greater than")]
    fn dynamic_size_rejects_bound_equal_to_lower_bound() {
        event_queue_cell_size(SerializedSize::Dynamic(8), Some(8));
    }

    #[test]
    #[should_panic(expected = "must be greater than")]
    fn dynamic_size_rejects_bound_below_lower_bound() {
        event_queue_cell_size(SerializedSize::Dynamic(8), Some(7));
    }
}
