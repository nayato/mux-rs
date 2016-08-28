use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use ::{Dentry, Dtab, Path};

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
    pub const MARKER_TAG: u32 = 0;
    // We reserve a tag for a default ping message so that we
    // can cache a full ping message and avoid encoding it
    // every time.
    pub const PING_TAG: u32 = 1;
    const MIN_TAG: u32 = PING_TAG + 1;
    pub const MAX_TAG: u32 = (1 << 23) - 1;
    pub const TAG_MSB: u32 = (1 << 23);

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

enum CaseMessage {
    Tinit {
        tag: u32,
        version: u16,
        headers: Vec<(Vec<u8>, Vec<u8>)>,
    },
    Rinit {
        tag: u32,
        version: u16,
        headers: Vec<(Vec<u8>, Vec<u8>)>,
    },
    /**
     * A transmit request message.
     *
     * Note, Treq messages are deprecated in favor of [[Tdispatch]] and will likely
     * be removed in a future version of mux.
     */
    Treq { tag: u32, req: Vec<u8> },
    /**
     * A reply to a `Treq` message.
     *
     * Note, Rreq messages are deprecated in favor of [[Rdispatch]] and will likely
     * be removed in a future version of mux.
     */
    RreqOk { tag: u32, reply: Vec<u8> },
    RreqError { tag: u32, error: String },
    RreqNack { tag: u32 },
    Tdispatch {
        tag: u32,
        contexts: Vec<(Vec<u8>, Vec<u8>)>,
        dst: Path,
        dtab: Dtab,
        req: Vec<u8>,
    },
    /** A reply to a `Tdispatch` message */
    RdispatchOk {
        tag: u32,
        contexts: Vec<(Vec<u8>, Vec<u8>)>,
        reply: Vec<u8>,
    },
    RdispatchError {
        tag: u32,
        contexts: Vec<(Vec<u8>, Vec<u8>)>,
        error: String,
    },
    RdispatchNack {
        tag: u32,
        contexts: Vec<(Vec<u8>, Vec<u8>)>,
    },
    /**
     * A fragment, as defined by the mux spec, is a message with its tag MSB
     * set to 1.
     */
    Fragment { typ: i8, tag: u32, buf: Vec<u8> },
    /** Indicates to the client to stop sending new requests. */
    Tdrain { tag: u32 },
    /** Response from the client to a `Tdrain` message */
    Rdrain { tag: u32 },
    /** Used to check liveness */
    Tping { tag: u32 },
    /**
     * We pre-encode a ping message with the reserved ping tag
     * (PingTag) in order to avoid re-encoding this frequently sent
     * message.
     */
    PreEncodedTping,
    /** Response to a `Tping` message */
    Rping { tag: u32 },
    /** Indicates that the corresponding T message produced an error. */
    Rerr { tag: u32, error: String },
    /**
     * Indicates that the `Treq` with the tag indicated by `which` has been discarded
     * by the client.
     */
    Tdiscarded { which: u32, why: String },
    Rdiscarded { tag: u32 },
    Tlease { unit: u8, how_long: u64 },
}

impl Message for CaseMessage {
    fn typ(&self) -> i8 {
        match *self {
            CaseMessage::Tinit { .. } => types::T_INIT,
            CaseMessage::Rinit { .. } => types::R_INIT,
            CaseMessage::Treq { .. } => types::T_REQ,
            CaseMessage::RreqOk { .. } |
            CaseMessage::RreqError { .. } |
            CaseMessage::RreqNack { .. } => types::R_REQ,
            CaseMessage::Tdispatch { .. } => types::T_DISPATCH,
            CaseMessage::RdispatchOk { .. } |
            CaseMessage::RdispatchError { .. } |
            CaseMessage::RdispatchNack { .. } => types::R_DISPATCH,
            CaseMessage::Fragment { typ, .. } => typ,
            CaseMessage::Tdrain { .. } => types::T_DRAIN,
            CaseMessage::Rdrain { .. } => types::R_DRAIN,
            CaseMessage::Tping { .. } => types::T_PING,
            CaseMessage::Rping { .. } => types::R_PING,
            // Use the old Rerr type in a transition period so that we
            // can be reasonably sure we remain backwards compatible with
            // old servers.
            CaseMessage::Rerr { .. } => types::BAD_R_ERR,
            // Use the old Tdiscarded type in a transition period so that we
            // can be reasonably sure we remain backwards compatible with
            // old servers.
            CaseMessage::Tdiscarded { .. } => types::BAD_T_DISCARDED,
            CaseMessage::Rdiscarded { .. } => types::R_DISCARDED,
            CaseMessage::Tlease { .. } => types::T_LEASE,
            CaseMessage::PreEncodedTping => 0,
        }
    }

    fn tag(&self) -> u32 {
        match *self {
            CaseMessage::Tinit { tag, .. } |
            CaseMessage::Rinit { tag, .. } |
            CaseMessage::Treq { tag, .. } |
            CaseMessage::RreqOk { tag, .. } |
            CaseMessage::RreqError { tag, .. } |
            CaseMessage::RreqNack { tag } |
            CaseMessage::Tdispatch { tag, .. } |
            CaseMessage::RdispatchOk { tag, .. } |
            CaseMessage::RdispatchError { tag, .. } |
            CaseMessage::RdispatchNack { tag, .. } |
            CaseMessage::Fragment { tag, .. } |
            CaseMessage::Tdrain { tag } |
            CaseMessage::Rdrain { tag } |
            CaseMessage::Tping { tag } |
            CaseMessage::Rping { tag } |
            CaseMessage::Rerr { tag, .. } |
            CaseMessage::Rdiscarded { tag } => tag,
            CaseMessage::Tdiscarded { .. } |
            CaseMessage::Tlease { .. } => 0,
            CaseMessage::PreEncodedTping => 0,
        }
    }

    fn buf(&self) -> Vec<u8> {
        match *self {
            CaseMessage::Tinit { version, ref headers, .. } => {
                init::encode(version, headers.clone())
            }
            CaseMessage::Rinit { version, ref headers, .. } => {
                init::encode(version, headers.clone())
            }
            CaseMessage::Treq { ref req, .. } => {
                let mut vec = vec![0];
                vec.extend_from_slice(&req[..]);
                vec
            }
            CaseMessage::RreqOk { ref reply, .. } => {
                let mut vec = vec![0];
                vec.extend_from_slice(&reply[..]);
                vec
            }
            CaseMessage::RreqError { ref error, .. } => {
                let mut vec = vec![1];
                let bytes = error.clone().into_bytes();
                vec.extend_from_slice(&bytes[..]);
                vec
            }
            CaseMessage::RreqNack { .. } => vec![2],
            CaseMessage::Tdispatch { ref contexts, ref dst, ref dtab, ref req, .. } => {
                let mut buf = Vec::new();
                buf.write_u16::<BigEndian>(contexts.len() as u16).unwrap();
                for pair in contexts {
                    let k = &pair.0;
                    let v = &pair.1;
                    buf.write_u16::<BigEndian>(k.len() as u16).unwrap();
                    buf.extend_from_slice(&k);
                    buf.write_u16::<BigEndian>(v.len() as u16).unwrap();
                    buf.extend_from_slice(&v);
                }

                buf.write_u16::<BigEndian>(dst.len() as u16).unwrap();
                let bytes = dst.clone().into_bytes();
                buf.extend_from_slice(&bytes[..]);

                buf.write_u16::<BigEndian>(dtab.len() as u16).unwrap();
                for dentry in &dtab.dentries {
                    let srcbytes = dentry.prefix.clone().into_bytes();
                    let treebytes = dentry.dst.clone().into_bytes();
                    buf.write_u16::<BigEndian>(srcbytes.len() as u16).unwrap();
                    buf.extend_from_slice(&srcbytes);
                    buf.write_u16::<BigEndian>(treebytes.len() as u16).unwrap();
                    buf.extend_from_slice(&treebytes);
                }
                buf.extend_from_slice(&req[..]);
                buf
            }
            CaseMessage::RdispatchOk { ref contexts, ref reply, .. } => {
                let mut buf = Vec::new();
                buf.push(0u8);
                buf.write_u16::<BigEndian>(contexts.len() as u16).unwrap();
                for pair in contexts {
                    let k = &pair.0;
                    let v = &pair.1;
                    buf.write_u16::<BigEndian>(k.len() as u16).unwrap();
                    buf.extend_from_slice(&k);
                    buf.write_u16::<BigEndian>(v.len() as u16).unwrap();
                    buf.extend_from_slice(&v);
                }
                buf.extend_from_slice(&reply[..]);
                buf
            }
            CaseMessage::RdispatchError { ref contexts, ref error, .. } => {
                let mut buf = Vec::new();
                buf.push(1u8);
                buf.write_u16::<BigEndian>(contexts.len() as u16).unwrap();
                for pair in contexts {
                    let k = &pair.0;
                    let v = &pair.1;
                    buf.write_u16::<BigEndian>(k.len() as u16).unwrap();
                    buf.extend_from_slice(&k);
                    buf.write_u16::<BigEndian>(v.len() as u16).unwrap();
                    buf.extend_from_slice(&v);
                }
                let bytes = error.clone().into_bytes();
                buf.extend_from_slice(&bytes[..]);
                buf
            }
            CaseMessage::RdispatchNack { ref contexts, .. } => {
                let mut buf = Vec::new();
                buf.push(2u8);
                buf.write_u16::<BigEndian>(contexts.len() as u16).unwrap();
                for pair in contexts {
                    let k = &pair.0;
                    let v = &pair.1;
                    buf.write_u16::<BigEndian>(k.len() as u16).unwrap();
                    buf.extend_from_slice(&k);
                    buf.write_u16::<BigEndian>(v.len() as u16).unwrap();
                    buf.extend_from_slice(&v);
                }
                buf
            }
            CaseMessage::Fragment { ref buf, .. } => buf.clone(),
            CaseMessage::Tdrain { .. } |
            CaseMessage::Rdrain { .. } |
            CaseMessage::Tping { .. } |
            CaseMessage::Rping { .. } |
            CaseMessage::Rdiscarded { .. } => vec![],
            CaseMessage::Rerr { ref error, .. } => error.clone().into_bytes(),
            CaseMessage::Tdiscarded { which, ref why } => {
                let mut arr = vec![(which >> 16 & 0xff) as u8,
                                   (which >> 8 & 0xff) as u8,
                                   (which & 0xff) as u8];
                let why = why.clone().into_bytes();
                arr.extend_from_slice(&why[..]);
                arr
            }
            CaseMessage::Tlease { unit, how_long } => {
                let mut buf = Vec::new();
                buf.push(unit);
                buf.write_u64::<BigEndian>(how_long).unwrap();
                buf
            }
            CaseMessage::PreEncodedTping => encode(CaseMessage::Tping { tag: tags::PING_TAG }),
        }
    }
}


fn encode(msg: CaseMessage) -> Vec<u8> {
    match msg {
        m @ CaseMessage::PreEncodedTping => m.buf(),
        m => {
            let tag = m.tag();
            let typ = m.typ();
            if tag < tags::MARKER_TAG || (tag & !tags::TAG_MSB) > tags::MAX_TAG {
                panic!("invalid tag number {}", tag);
            }

            let mut head = vec![typ as u8,
                                (tag >> 16 & 0xff) as u8,
                                (tag >> 8 & 0xff) as u8,
                                (tag & 0xff) as u8];

            head.extend_from_slice(&m.buf()[..]);
            head
        }
    }
}
