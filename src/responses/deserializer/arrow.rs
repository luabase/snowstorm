use arrow_format;
use std::io::{Read, Seek};

#[derive(Debug, Clone)]
pub struct BufferMetadata {
    /// The schema that is read from the stream's first message
    pub schema: Schema,

    /// The IPC version of the stream
    pub version: arrow_format::ipc::MetadataVersion,

    /// The IPC fields tracking dictionaries
    pub ipc_schema: IpcSchema,
}

pub(super) fn read_buffer_metadata<R: Read + Seek>(reader: &mut R) -> Result<BufferMetadata> {
    // check if header contain the correct magic bytes
    let mut magic_buffer: [u8; 6] = [0; 6];
    let start = reader.seek(SeekFrom::Current(0))?;
    reader.read_exact(&mut magic_buffer)?;
    if magic_buffer != ARROW_MAGIC {
        return Err(Error::from(OutOfSpecKind::InvalidHeader));
    }

    let (end, footer_len) = read_footer_len(reader)?;

    // read footer
    reader.seek(SeekFrom::End(-10 - footer_len as i64))?;

    let mut serialized_footer = vec![];
    serialized_footer.try_reserve(footer_len)?;
    reader
        .by_ref()
        .take(footer_len as u64)
        .read_to_end(&mut serialized_footer)?;

    deserialize_footer(&serialized_footer, end - start)
}
