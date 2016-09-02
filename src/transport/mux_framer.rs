/**
 * Defines a [[com.twitter.finagle.transport.Transport]] which allows a
 * mux session to be shared between multiple tag streams. The transport splits
 * mux messages into fragments with a size defined by a parameter. Writes are
 * then interleaved to achieve equity and goodput over the entire stream.
 * Fragments are aggregated into complete mux messages when read. The fragment size
 * is negotiated when a mux session is initialized.
 *
 * @see [[com.twitter.finagle.mux.Handshake]] for usage details.
 *
 * @note Our current implementation does not offer any mechanism to resize
 * the window after a session is established. However, it is possible to
 * compose a flow control algorithm over this which can dynamically control
 * the size of `window`.
 */
/**
 * Defines mux framer keys and values exchanged as part of a
 * mux session header during initialization.
 */
mod header {
    use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
    use std::io::Cursor;
    // val KeyBuf: Buf = Buf.Utf8("mux-framer")

    /**
     * Returns a header value with the given frame `size` encoded.
     */
    fn encode_frame_size(size: u32) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.write_u32::<BigEndian>(size).unwrap();
        buf
    }

    /**
     * Extracts frame size from the `buf`.
    */
    fn decode_frame_size(buf: Vec<u8>) -> u32 {
        let mut rdr = Cursor::new(buf);
        rdr.read_u32::<BigEndian>().unwrap()
    }
}
