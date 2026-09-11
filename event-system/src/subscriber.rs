use {
    crate::backend,
    std::{marker::PhantomData, path::PathBuf},
    wincode::{Deserialize, ReadResult},
    wincode_dynamic::{Decoder, Fields, RootSchema},
};

/// A [`StreamExplorer`] listens to a given directory for event streams that are created
/// by [`EventSystem::create_stream`](crate::EventSystem::create_stream) in the same
/// directory.
///
/// To subscribe to a stream, simply use the [`StreamExplorer::available_streams] API
/// which yields an iterator over streams that can be subscribed to.
pub struct StreamExplorer(backend::StreamExplorer);

impl StreamExplorer {
    pub fn new(event_system_directory: PathBuf) -> Self {
        Self(backend::StreamExplorer::new(event_system_directory))
    }

    /// Iterates over streams that are available to be subscribed to.
    ///
    /// A stream can be subscribed to with [`AvailableStream::try_connect_dynamic`] or
    /// [`AvailableStream::try_connect_typed`].
    pub fn available_streams(&self) -> impl Iterator<Item = AvailableStream> + '_ {
        self.0.available_streams().map(AvailableStream)
    }
}

/// Marker for dynamically reflecting over stream messages.
/// See [`StreamSubscriber`] for details on subscriber modes.
pub struct Dynamic;

/// Marker for decoding stream messages into a statically typed `T`.
/// See [`StreamSubscriber`] for details on subscriber modes.
pub struct Typed<T> {
    _marker: PhantomData<T>,
}

/// A subscriber to a specific event stream. This
/// subscriber can be created with either [`Dynamic`] mode
/// which lets users dynamically reflect over messages on the stream,
/// or the [`Typed<T>`] mode which returns the T directly if the user
/// knows what T is at compile time.
pub struct StreamSubscriber<Mode> {
    backend: backend::StreamSubscriber,
    t: PhantomData<Mode>,
}

impl<T> StreamSubscriber<T> {
    /// The name of the stream the subscriber is listening on.
    pub fn stream_name(&self) -> &str {
        self.backend.stream_name()
    }
    /// The name of the type that is sent on the stream.
    pub fn type_name(&self) -> &str {
        self.backend.type_name()
    }
}

impl StreamSubscriber<Dynamic> {
    /// Returns a message if there is any unseen message in the stream.
    pub fn try_recv(&mut self) -> Result<DynamicStreamMessage<'_>, TryRecvError> {
        let payload = self.backend.try_recv()?;
        let schema = self.backend.schema();

        Ok(DynamicStreamMessage::new(schema, payload))
    }

    fn new(backend: backend::StreamSubscriber) -> Self {
        Self {
            backend,
            t: PhantomData,
        }
    }
}

impl<T> StreamSubscriber<Typed<T>>
where
    T: for<'de> Deserialize<'de, Dst = T>,
{
    /// Returns a message if there is any unseen message in the stream.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let payload = self.backend.try_recv()?;

        wincode::deserialize(&payload[..]).map_err(|_| {
            // this error should never happen since the schema was validated
            // when subscribing to the stream in `try_connect_typed`
            TryRecvError::InvalidEncoding
        })
    }
}

impl<T> StreamSubscriber<Typed<T>> {
    fn new(backend: backend::StreamSubscriber) -> Self {
        Self {
            backend,
            t: PhantomData,
        }
    }
}

/// A discovered stream that can be connected to.
pub struct AvailableStream(backend::AvailableStream);

impl AvailableStream {
    /// The name of the available stream.
    pub fn stream_name(&self) -> &str {
        self.0.stream_name()
    }

    /// The name of the type that is sent on the stream.
    pub fn type_name(&self) -> &str {
        self.0.type_name()
    }

    /// Attempts to connect to the stream with dynamic decoding of the messages.
    ///
    /// This method should be used over [`try_connect_typed`](Self::try_connect_typed) when you don't know what
    /// concrete rust type is sent on the stream.
    pub fn try_connect_dynamic(self) -> Result<StreamSubscriber<Dynamic>, TryConnectError> {
        self.0.try_connect().map(StreamSubscriber::<Dynamic>::new)
    }

    /// Attempts to connect to the stream with static decoding of the messages into a concrete type `T`.
    ///
    /// If you don't know at compile time what type the stream contains you should instead use
    /// [`try_connect_dynamic`](Self::try_connect_dynamic).
    #[allow(clippy::result_large_err)]
    pub fn try_connect_typed<T: wincode_dynamic::SchemaDynamic>(
        self,
    ) -> Result<StreamSubscriber<Typed<T>>, TryConnectTypedError> {
        let expected_schema = T::schema();
        let actual_schema = self.0.stream_schema();

        if expected_schema != *actual_schema {
            return Err(TryConnectTypedError::SchemaMismatch {
                expected: expected_schema,
                actual: actual_schema.clone(),
            });
        }

        self.0
            .try_connect()
            .map(StreamSubscriber::<Typed<T>>::new)
            .map_err(TryConnectTypedError::Connection)
    }
}

/// Errors that can arise when trying to receive an event on a specific event stream
/// through a subscriber.
#[derive(thiserror::Error, Debug)]
pub enum TryRecvError {
    #[error("the stream has no new message")]
    Empty,
    #[error("the received message could not be deserialized")]
    InvalidEncoding,
}

#[derive(Debug, thiserror::Error)]
pub enum TryConnectTypedError {
    #[error("stream schema does not match the requested type")]
    SchemaMismatch {
        expected: RootSchema,
        actual: RootSchema,
    },

    #[error(transparent)]
    Connection(#[from] TryConnectError),
}

#[derive(Debug, thiserror::Error)]
pub enum TryConnectError {}

/// A dynamic message received on a dynamically typed stream [`StreamSubscriber<Dynamic>`].
pub struct DynamicStreamMessage<'a> {
    decoder: Decoder<'a>,
    payload: Box<[u8]>,
}

impl<'a> DynamicStreamMessage<'a> {
    /// Decodes the dynamic message into a [`DecodedMessage`]
    pub fn decode<'de>(&'de self) -> ReadResult<DecodedMessage<'a, 'de>> {
        Ok(match &self.decoder {
            Decoder::Struct(schema_decoder) => DecodedMessage::Struct {
                fields: schema_decoder.fields(self.payload.as_ref()),
            },
            Decoder::Enum(enum_decoder) => {
                let variant_decoder = enum_decoder.decode_variant(self.payload.as_ref())?;
                DecodedMessage::Enum {
                    variant_name: variant_decoder.variant_name(),
                    fields: variant_decoder.fields(),
                }
            }
        })
    }

    fn new(schema: &'a RootSchema, payload: Box<[u8]>) -> Self {
        Self {
            decoder: Decoder::new(schema),
            payload,
        }
    }
}

/// A decoded dynamically typed message. Messages can either be an enum or a struct.
pub enum DecodedMessage<'a, 'de> {
    Struct {
        fields: Fields<'a, 'de, &'de [u8]>,
    },
    Enum {
        fields: Fields<'a, 'de, &'de [u8]>,
        variant_name: &'a str,
    },
}
