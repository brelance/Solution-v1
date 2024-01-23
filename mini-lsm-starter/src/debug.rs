use bytes::Bytes;



pub fn as_bytes(slice: &[u8]) -> Bytes {
    Bytes::copy_from_slice(slice)
}