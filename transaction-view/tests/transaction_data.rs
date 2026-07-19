use agave_transaction_view::transaction_data::TransactionData;
use bytes::Bytes;

#[test]
fn transaction_data_is_implemented_for_owned_bytes_type() {
    const RANDOM_BYTES: Bytes = Bytes::from_static(b"hello world!");
    TransactionData::data(&RANDOM_BYTES);
}

#[test]
fn transaction_data_is_implemented_for_borrowed_bytes_type() {
    const RANDOM_BYTES: Bytes = Bytes::from_static(b"hello world!");
    let bytes_ref: &Bytes = &RANDOM_BYTES;

    TransactionData::data(&bytes_ref);
}
