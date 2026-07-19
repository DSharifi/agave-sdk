/// Trait for accessing transaction data from an abstract byte container.
pub trait TransactionData {
    /// Returns a reference to the serialized transaction data.
    fn data(&self) -> &[u8];
}

impl<D: AsRef<[u8]>> TransactionData for D {
    #[inline]
    fn data(&self) -> &[u8] {
        self.as_ref()
    }
}
