/**
 * Indicates that encoding or decoding of a Mux message failed.
 * Reason for failure should be provided by the `why` string.
 */
// case class BadMessageException(why: String) extends Exception(why)
/**
 * Documentation details are in the [[com.twitter.finagle.mux]] package object.
 */
trait Message {
    /**
     * Values should correspond to the constants defined in
     * [[com.twitter.finagle.mux.Message.Types]]
     */
    fn typ(&self) -> i8;

    /** Only 3 of its bytes are used. */
    fn tag(&self) -> u32;

    /**
     * The body of the message omitting size, typ, and tag.
     */
    fn buf(&self) -> Vec<u8>;
}

mod types {
    // Application messages:
    const T_REQ: i8 = 1;
    const R_REQ: i8 = -1;

    const T_DISPATCH: i8 = 2;
    const R_DISPATCH: i8 = -2;

    // Control messages:
    const T_DRAIN: i8 = 64;
    const R_DRAIN: i8 = -64;
    const T_PING: i8 = 65;
    const R_PING: i8 = -65;

    const T_DISCARDED: i8 = 66;
    const R_DISCARDED: i8 = -66;

    const T_LEASE: i8 = 67;

    const T_INIT: i8 = 68;
    const R_INIT: i8 = -68;

    const R_ERR: i8 = -128;

    // Old implementation flukes.
    const BAD_T_DISCARDED: i8 = -62;
    const BAD_R_ERR: i8 = 127;
}

mod tags {
    const MARKER_TAG: u32 = 0;
    // We reserve a tag for a default ping message so that we
    // can cache a full ping message and avoid encoding it
    // every time.
    const PING_TAG: u32 = 1;
    const MIN_TAG: u32 = PING_TAG + 1;
    const MAX_TAG: u32 = (1 << 23) - 1;
    const TAG_MSB: u32 = (1 << 23);

    fn extract_type(header: u32) -> i8 {
        (header >> 24 & 0xff) as i8
    }

    fn extract_tag(header: u32) -> u32 {
        header & 0x00ffffff
    }

    fn is_fragment(tag: u32) -> bool {
        (tag >> 23 & 1) == 1
    }

    fn set_msb(tag: u32) -> u32 {
        tag | TAG_MSB
    }
}

// private def mkByte(b: Byte) = Buf.ByteArray.Owned(Array(b))

// private val bufOfChar = Array[Buf](mkByte(0), mkByte(1), mkByte(2))

trait EmptyMessage: Message {
    fn buf(&self) -> Vec<u8> {
        Vec::new()
    }
}

trait MarkerMessage: Message {
    fn tag(&self) -> u32 {
        0
    }
}

mod init {
    use std::io::Read;
    use std::mem::transmute;

    fn encode(version: u16, headers: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<u8> {
        let mut size: usize = 2; // 2 bytes for version
        for pair in &headers {
            // 8 bytes for length encoding of k, v
            size += 8 + pair.0.len() + pair.1.len();
        }
        let mut buf = Vec::with_capacity(size);
        let bytes: [u8; 2] = unsafe { transmute(version.to_be()) };
        buf.extend_from_slice(&bytes);
        for pair in &headers {
            let k = &pair.0;
            let v = &pair.1;
            let bytes: [u8; 4] = unsafe { transmute((k.len() as u32).to_be()) };
            buf.extend_from_slice(&bytes);
            buf.extend_from_slice(&k);
            let bytes: [u8; 4] = unsafe { transmute((v.len() as u32).to_be()) };
            buf.extend_from_slice(&bytes);
            buf.extend_from_slice(&v);
        }
        buf
    }

    fn decode(buf: Vec<u8>) -> (u16, Vec<(Vec<u8>, Vec<u8>)>) {
        let mut buf = &buf.as_slice()[..];
        let mut bytes: [u8; 2] = [0; 2];
        buf.read_exact(&mut bytes).unwrap();
        let version: u16 = u16::from_be(unsafe { transmute::<[u8; 2], u16>(bytes) });
        let mut headers: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        while buf.len() > 0 {
            let mut bytes: [u8; 4] = [0; 4];
            buf.read_exact(&mut bytes).unwrap();
            let kl = u32::from_be(unsafe { transmute::<[u8; 4], u32>(bytes) }) as usize;
            let mut k: Vec<u8> = Vec::new();
            k.resize(kl, 0);
            buf.read_exact(&mut k).unwrap();
            buf.read_exact(&mut bytes).unwrap();
            let vl = u32::from_be(unsafe { transmute::<[u8; 4], u32>(bytes) }) as usize;
            let mut v: Vec<u8> = Vec::new();
            v.resize(vl, 0);
            buf.read_exact(&mut v).unwrap();
            headers.push((k, v));
        }
        (version, headers)
    }

    #[test]
    fn test_init() {
        let version: u16 = 0x1020;
        let headers = vec![(vec![1], vec![2, 3]),
                           (vec![4, 5, 6], vec![7, 8, 9, 10]),
                           (vec![11, 12, 13], vec![14, 15])];
        let buf = encode(version, headers.clone());
        let (got_version, got_headers) = decode(buf);
        assert_eq!(version, got_version);
        assert_eq!(headers, got_headers);
    }
}
