use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use ::{Dentry, Dtab, Path};

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
    pub const T_REQ: i8 = 1;
    pub const R_REQ: i8 = -1;

    pub const T_DISPATCH: i8 = 2;
    pub const R_DISPATCH: i8 = -2;

    // Control messages:
    pub const T_DRAIN: i8 = 64;
    pub const R_DRAIN: i8 = -64;
    pub const T_PING: i8 = 65;
    pub const R_PING: i8 = -65;

    pub const T_DISCARDED: i8 = 66;
    pub const R_DISCARDED: i8 = -66;

    pub const T_LEASE: i8 = 67;

    pub const T_INIT: i8 = 68;
    pub const R_INIT: i8 = -68;

    pub const R_ERR: i8 = -128;

    // Old implementation flukes.
    pub const BAD_T_DISCARDED: i8 = -62;
    pub const BAD_R_ERR: i8 = 127;
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
    use std::io::Cursor;
    use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

    pub fn encode(version: u16, headers: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.write_u16::<BigEndian>(version).unwrap();
        for pair in &headers {
            let k = &pair.0;
            let v = &pair.1;
            buf.write_u32::<BigEndian>(k.len() as u32).unwrap();
            buf.extend_from_slice(&k);
            buf.write_u32::<BigEndian>(v.len() as u32).unwrap();
            buf.extend_from_slice(&v);
        }
        buf
    }

    pub fn decode(buf: Vec<u8>) -> (u16, Vec<(Vec<u8>, Vec<u8>)>) {
        let len = buf.len() as u64;
        let mut rdr = Cursor::new(buf);
        let version = rdr.read_u16::<BigEndian>().unwrap();
        let mut headers: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        while rdr.position() < len {
            let kl = rdr.read_u32::<BigEndian>().unwrap() as usize;
            let mut k: Vec<u8> = Vec::new();
            k.resize(kl, 0);
            rdr.read_exact(&mut k).unwrap();
            let vl = rdr.read_u32::<BigEndian>().unwrap() as usize;
            let mut v: Vec<u8> = Vec::new();
            v.resize(vl, 0);
            rdr.read_exact(&mut v).unwrap();
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

struct Tinit {
    tag: u32,
    version: u16,
    headers: Vec<(Vec<u8>, Vec<u8>)>,
}

impl Message for Tinit {
    fn typ(&self) -> i8 {
        types::T_INIT
    }

    fn tag(&self) -> u32 {
        self.tag
    }

    fn buf(&self) -> Vec<u8> {
        init::encode(self.version, self.headers.clone())
    }
}

struct Rinit {
    tag: u32,
    version: u16,
    headers: Vec<(Vec<u8>, Vec<u8>)>,
}

impl Message for Rinit {
    fn typ(&self) -> i8 {
        types::R_INIT
    }

    fn tag(&self) -> u32 {
        self.tag
    }

    fn buf(&self) -> Vec<u8> {
        init::encode(self.version, self.headers.clone())
    }
}

/**
   * A transmit request message.
   *
   * Note, Treq messages are deprecated in favor of [[Tdispatch]] and will likely
   * be removed in a future version of mux.
   */
struct Treq {
    tag: u32,
    req: Vec<u8>,
}

impl Message for Treq {
    fn typ(&self) -> i8 {
        types::T_REQ
    }

    fn tag(&self) -> u32 {
        self.tag
    }

    fn buf(&self) -> Vec<u8> {
        let mut vec = vec![0];
        vec.extend_from_slice(&self.req[..]);
        vec
    }
}

/**
 * A reply to a `Treq` message.
 *
 * Note, Rreq messages are deprecated in favor of [[Rdispatch]] and will likely
 * be removed in a future version of mux.
 */
struct RreqOk {
    tag: u32,
    reply: Vec<u8>,
}

impl Message for RreqOk {
    fn typ(&self) -> i8 {
        types::R_REQ
    }

    fn tag(&self) -> u32 {
        self.tag
    }

    fn buf(&self) -> Vec<u8> {
        let mut vec = vec![0];
        vec.extend_from_slice(&self.reply[..]);
        vec
    }
}

struct RreqError {
    tag: u32,
    error: String,
}

impl Message for RreqError {
    fn typ(&self) -> i8 {
        types::R_REQ
    }

    fn tag(&self) -> u32 {
        self.tag
    }

    fn buf(&self) -> Vec<u8> {
        let mut vec = vec![1];
        let bytes = self.error.clone().into_bytes();
        vec.extend_from_slice(&bytes[..]);
        vec
    }
}

struct RreqNack {
    tag: u32,
}

impl Message for RreqNack {
    fn typ(&self) -> i8 {
        types::R_REQ
    }

    fn tag(&self) -> u32 {
        self.tag
    }

    fn buf(&self) -> Vec<u8> {
        vec![2]
    }
}

struct Tdispatch {
    tag: u32,
    contexts: Vec<(Vec<u8>, Vec<u8>)>,
    dst: Path,
    dtab: Dtab,
    req: Vec<u8>,
}

impl Message for Tdispatch {
    fn typ(&self) -> i8 {
        types::T_DISPATCH
    }

    fn tag(&self) -> u32 {
        self.tag
    }

    fn buf(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.write_u16::<BigEndian>(self.contexts.len() as u16).unwrap();
        for pair in &self.contexts {
            let k = &pair.0;
            let v = &pair.1;
            buf.write_u16::<BigEndian>(k.len() as u16).unwrap();
            buf.extend_from_slice(&k);
            buf.write_u16::<BigEndian>(v.len() as u16).unwrap();
            buf.extend_from_slice(&v);
        }

        buf.write_u16::<BigEndian>(self.dst.len() as u16).unwrap();
        let bytes = self.dst.clone().into_bytes();
        buf.extend_from_slice(&bytes[..]);

        buf.write_u16::<BigEndian>(self.dtab.len() as u16).unwrap();
        for dentry in &self.dtab.dentries {
            let srcbytes = dentry.prefix.clone().into_bytes();
            let treebytes = dentry.dst.clone().into_bytes();
            buf.write_u16::<BigEndian>(srcbytes.len() as u16).unwrap();
            buf.extend_from_slice(&srcbytes);
            buf.write_u16::<BigEndian>(treebytes.len() as u16).unwrap();
            buf.extend_from_slice(&treebytes);
        }
        buf.extend_from_slice(&self.req[..]);
        buf
    }
}

struct RdispatchOk {
    tag: u32,
    contexts: Vec<(Vec<u8>, Vec<u8>)>,
    reply: Vec<u8>,
}

impl Message for RdispatchOk {
    fn typ(&self) -> i8 {
        types::R_DISPATCH
    }

    fn tag(&self) -> u32 {
        self.tag
    }

    fn buf(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(0u8);
        buf.write_u16::<BigEndian>(self.contexts.len() as u16).unwrap();
        for pair in &self.contexts {
            let k = &pair.0;
            let v = &pair.1;
            buf.write_u16::<BigEndian>(k.len() as u16).unwrap();
            buf.extend_from_slice(&k);
            buf.write_u16::<BigEndian>(v.len() as u16).unwrap();
            buf.extend_from_slice(&v);
        }
        buf.extend_from_slice(&self.reply[..]);
        buf
    }
}

struct RdispatchError {
    tag: u32,
    contexts: Vec<(Vec<u8>, Vec<u8>)>,
    error: String,
}

impl Message for RdispatchError {
    fn typ(&self) -> i8 {
        types::R_DISPATCH
    }

    fn tag(&self) -> u32 {
        self.tag
    }

    fn buf(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(1u8);
        buf.write_u16::<BigEndian>(self.contexts.len() as u16).unwrap();
        for pair in &self.contexts {
            let k = &pair.0;
            let v = &pair.1;
            buf.write_u16::<BigEndian>(k.len() as u16).unwrap();
            buf.extend_from_slice(&k);
            buf.write_u16::<BigEndian>(v.len() as u16).unwrap();
            buf.extend_from_slice(&v);
        }
        let bytes = self.error.clone().into_bytes();
        buf.extend_from_slice(&bytes[..]);
        buf
    }
}

struct RdispatchNack {
    tag: u32,
    contexts: Vec<(Vec<u8>, Vec<u8>)>,
}

impl Message for RdispatchNack {
    fn typ(&self) -> i8 {
        types::R_DISPATCH
    }

    fn tag(&self) -> u32 {
        self.tag
    }

    fn buf(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(2u8);
        buf.write_u16::<BigEndian>(self.contexts.len() as u16).unwrap();
        for pair in &self.contexts {
            let k = &pair.0;
            let v = &pair.1;
            buf.write_u16::<BigEndian>(k.len() as u16).unwrap();
            buf.extend_from_slice(&k);
            buf.write_u16::<BigEndian>(v.len() as u16).unwrap();
            buf.extend_from_slice(&v);
        }
        buf
    }
}

/**
 * A fragment, as defined by the mux spec, is a message with its tag MSB
 * set to 1.
 */
struct Fragment {
    typ: i8,
    tag: u32,
    buf: Vec<u8>,
}

impl Message for Fragment {
    fn typ(&self) -> i8 {
        self.typ
    }

    fn tag(&self) -> u32 {
        self.tag
    }

    fn buf(&self) -> Vec<u8> {
        self.buf.clone()
    }
}

/** Indicates to the client to stop sending new requests. */
struct Tdrain {
    tag: u32,
}

impl Message for Tdrain {
    fn typ(&self) -> i8 {
        types::T_DRAIN
    }

    fn tag(&self) -> u32 {
        self.tag
    }

    fn buf(&self) -> Vec<u8> {
        Vec::new()
    }
}

/** Response from the client to a `Tdrain` message */
struct Rdrain {
    tag: u32,
}

impl Message for Rdrain {
    fn typ(&self) -> i8 {
        types::R_DRAIN
    }

    fn tag(&self) -> u32 {
        self.tag
    }

    fn buf(&self) -> Vec<u8> {
        Vec::new()
    }
}

/** Used to check liveness */
struct Tping {
    tag: u32,
}

impl Message for Tping {
    fn typ(&self) -> i8 {
        types::T_PING
    }

    fn tag(&self) -> u32 {
        self.tag
    }

    fn buf(&self) -> Vec<u8> {
        Vec::new()
    }
}
