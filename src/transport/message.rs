use std::io::Cursor;
use std::io::Read;

use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use ::{Dentry, Dtab, Path};

mod types {
    // Application messages:
    pub const TREQ: i8 = 1;
    pub const RREQ: i8 = -1;

    pub const TDISPATCH: i8 = 2;
    pub const RDISPATCH: i8 = -2;

    // Control messages:
    pub const TDRAIN: i8 = 64;
    pub const RDRAIN: i8 = -64;
    pub const TPING: i8 = 65;
    pub const RPING: i8 = -65;

    pub const TDISCARDED: i8 = 66;
    pub const RDISCARDED: i8 = -66;

    pub const TLEASE: i8 = 67;

    pub const TINIT: i8 = 68;
    pub const RINIT: i8 = -68;

    pub const RERR: i8 = -128;

    // Old implementation flukes.
    pub const BAD_TDISCARDED: i8 = -62;
    pub const BAD_RERR: i8 = 127;
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
    use std::io::Cursor;
    use std::io::Read;
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

enum Message {
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

impl Message {
    fn typ(&self) -> i8 {
        match *self {
            Message::Tinit { .. } => types::TINIT,
            Message::Rinit { .. } => types::RINIT,
            Message::Treq { .. } => types::TREQ,
            Message::RreqOk { .. } |
            Message::RreqError { .. } |
            Message::RreqNack { .. } => types::RREQ,
            Message::Tdispatch { .. } => types::TDISPATCH,
            Message::RdispatchOk { .. } |
            Message::RdispatchError { .. } |
            Message::RdispatchNack { .. } => types::RDISPATCH,
            Message::Fragment { typ, .. } => typ,
            Message::Tdrain { .. } => types::TDRAIN,
            Message::Rdrain { .. } => types::RDRAIN,
            Message::Tping { .. } => types::TPING,
            Message::Rping { .. } => types::RPING,
            // Use the old Rerr type in a transition period so that we
            // can be reasonably sure we remain backwards compatible with
            // old servers.
            Message::Rerr { .. } => types::BAD_RERR,
            // Use the old Tdiscarded type in a transition period so that we
            // can be reasonably sure we remain backwards compatible with
            // old servers.
            Message::Tdiscarded { .. } => types::BAD_TDISCARDED,
            Message::Rdiscarded { .. } => types::RDISCARDED,
            Message::Tlease { .. } => types::TLEASE,
            Message::PreEncodedTping => 0,
        }
    }

    fn tag(&self) -> u32 {
        match *self {
            Message::Tinit { tag, .. } |
            Message::Rinit { tag, .. } |
            Message::Treq { tag, .. } |
            Message::RreqOk { tag, .. } |
            Message::RreqError { tag, .. } |
            Message::RreqNack { tag } |
            Message::Tdispatch { tag, .. } |
            Message::RdispatchOk { tag, .. } |
            Message::RdispatchError { tag, .. } |
            Message::RdispatchNack { tag, .. } |
            Message::Fragment { tag, .. } |
            Message::Tdrain { tag } |
            Message::Rdrain { tag } |
            Message::Tping { tag } |
            Message::Rping { tag } |
            Message::Rerr { tag, .. } |
            Message::Rdiscarded { tag } => tag,
            Message::Tdiscarded { .. } |
            Message::Tlease { .. } => 0,
            Message::PreEncodedTping => 0,
        }
    }

    fn buf(&self) -> Vec<u8> {
        match *self {
            Message::Tinit { version, ref headers, .. } => init::encode(version, headers.clone()),
            Message::Rinit { version, ref headers, .. } => init::encode(version, headers.clone()),
            Message::Treq { ref req, .. } => {
                let mut vec = vec![0];
                vec.extend_from_slice(&req[..]);
                vec
            }
            Message::RreqOk { ref reply, .. } => {
                let mut vec = vec![0];
                vec.extend_from_slice(&reply[..]);
                vec
            }
            Message::RreqError { ref error, .. } => {
                let mut vec = vec![1];
                let bytes = error.clone().into_bytes();
                vec.extend_from_slice(&bytes[..]);
                vec
            }
            Message::RreqNack { .. } => vec![2],
            Message::Tdispatch { ref contexts, ref dst, ref dtab, ref req, .. } => {
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
                for dentry in dtab {
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
            Message::RdispatchOk { ref contexts, ref reply, .. } => {
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
            Message::RdispatchError { ref contexts, ref error, .. } => {
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
            Message::RdispatchNack { ref contexts, .. } => {
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
            Message::Fragment { ref buf, .. } => buf.clone(),
            Message::Tdrain { .. } |
            Message::Rdrain { .. } |
            Message::Tping { .. } |
            Message::Rping { .. } |
            Message::Rdiscarded { .. } => vec![],
            Message::Rerr { ref error, .. } => error.clone().into_bytes(),
            Message::Tdiscarded { which, ref why } => {
                let mut arr = vec![(which >> 16 & 0xff) as u8,
                                   (which >> 8 & 0xff) as u8,
                                   (which & 0xff) as u8];
                let why = why.clone().into_bytes();
                arr.extend_from_slice(&why[..]);
                arr
            }
            Message::Tlease { unit, how_long } => {
                let mut buf = Vec::new();
                buf.push(unit);
                buf.write_u64::<BigEndian>(how_long).unwrap();
                buf
            }
            Message::PreEncodedTping => encode(Message::Tping { tag: tags::PING_TAG }),
        }
    }
}

fn encode(msg: Message) -> Vec<u8> {
    match msg {
        m @ Message::PreEncodedTping => m.buf(),
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

fn decode_treq(tag: u32, buf: Vec<u8>) -> Message {
    if buf.len() < 1 {
        panic!("short Treq");
    }

    let mut rdr = Cursor::new(buf);
    let mut nkeys = [0u8];
    rdr.read_exact(&mut nkeys).unwrap();
    if nkeys[0] != 0 {
        panic!("Treq: too many keys");
    }
    let mut req: Vec<u8> = Vec::new();
    rdr.read_to_end(&mut req).unwrap();
    Message::Treq {
        tag: tag,
        req: req,
    }
}

fn decode_contexts(rdr: &mut Cursor<Vec<u8>>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut contexts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut n = rdr.read_u16::<BigEndian>().unwrap();
    if n == 0 {
        return contexts;
    }
    while n > 0 {
        let kl = rdr.read_u16::<BigEndian>().unwrap() as usize;
        let mut k: Vec<u8> = Vec::new();
        k.resize(kl, 0);
        rdr.read_exact(&mut k).unwrap();
        let vl = rdr.read_u16::<BigEndian>().unwrap() as usize;
        let mut v: Vec<u8> = Vec::new();
        v.resize(vl, 0);
        rdr.read_exact(&mut v).unwrap();
        contexts.push((k, v));
        n -= 1;
    }
    contexts
}

fn decode_tdispatch(tag: u32, buf: Vec<u8>) -> Message {
    let mut rdr = Cursor::new(buf);
    let contexts = decode_contexts(&mut rdr);
    let ndst = rdr.read_u16::<BigEndian>().unwrap();
    let mut dst = String::new();
    if ndst > 0 {
        let mut s: Vec<u8> = Vec::new();
        s.resize(ndst as usize, 0);
        rdr.read_exact(&mut s).unwrap();
        dst = String::from_utf8(s).unwrap();
    }

    let mut nd = rdr.read_u16::<BigEndian>().unwrap();
    let mut dtab: Dtab = Vec::new();
    while nd > 0 {
        let sl = rdr.read_u16::<BigEndian>().unwrap() as usize;
        let mut s: Vec<u8> = Vec::new();
        s.resize(sl as usize, 0);
        rdr.read_exact(&mut s).unwrap();
        let src = String::from_utf8(s).unwrap();
        let dl = rdr.read_u16::<BigEndian>().unwrap() as usize;
        let mut d: Vec<u8> = Vec::new();
        d.resize(dl as usize, 0);
        rdr.read_exact(&mut d).unwrap();
        let dst = String::from_utf8(d).unwrap();
        dtab.push(Dentry {
            prefix: src,
            dst: dst,
        });
        nd -= 1;
    }
    let mut req: Vec<u8> = Vec::new();
    rdr.read_to_end(&mut req).unwrap();
    Message::Tdispatch {
        tag: tag,
        contexts: contexts,
        dst: dst,
        dtab: dtab,
        req: req,
    }
}

fn decode_rdispatch(tag: u32, buf: Vec<u8>) -> Message {
    let mut rdr = Cursor::new(buf);
    let mut status = [0u8];
    rdr.read_exact(&mut status).unwrap();
    let contexts = decode_contexts(&mut rdr);
    let mut rest: Vec<u8> = Vec::new();
    rdr.read_to_end(&mut rest).unwrap();
    match status[0] {
        0 => {
            Message::RdispatchOk {
                tag: tag,
                contexts: contexts,
                reply: rest,
            }
        }
        1 => {
            Message::RdispatchError {
                tag: tag,
                contexts: contexts,
                error: String::from_utf8(rest).unwrap(),
            }
        }
        2 => {
            Message::RdispatchNack {
                tag: tag,
                contexts: contexts,
            }
        }
        _ => panic!("invalid Rdispatch status"),
    }
}

fn decode_rreq(tag: u32, buf: Vec<u8>) -> Message {
    if buf.len() < 1 {
        panic!("short Rreq");
    }
    let mut rdr = Cursor::new(buf);
    let mut status = [0u8];
    rdr.read_exact(&mut status).unwrap();
    let mut rest: Vec<u8> = Vec::new();
    rdr.read_to_end(&mut rest).unwrap();
    match status[0] {
        0 => {
            Message::RreqOk {
                tag: tag,
                reply: rest,
            }
        }
        1 => {
            Message::RreqError {
                tag: tag,
                error: String::from_utf8(rest).unwrap(),
            }
        }
        2 => Message::RreqNack { tag: tag },
        _ => panic!("invalid Rreq status"),
    }
}

fn decode_tdiscarded(buf: Vec<u8>) -> Message {
    if buf.len() < 3 {
        panic!("short Tdiscarded message");
    }
    let mut rdr = Cursor::new(buf);
    let mut bytes = [0; 3];
    rdr.read_exact(&mut bytes).unwrap();
    let which: u32 = (((bytes[0] & 0xff) as u32) << 16) | (((bytes[1] & 0xff) as u32) << 8) |
                     (bytes[2] & 0xff) as u32;
    let mut why: Vec<u8> = Vec::new();
    rdr.read_to_end(&mut why).unwrap();
    Message::Tdiscarded {
        which: which,
        why: String::from_utf8(why).unwrap(),
    }
}

fn decode_tlease(buf: Vec<u8>) -> Message {
    if buf.len() < 9 {
        panic!("short Tlease message");
    }
    let mut rdr = Cursor::new(buf);
    let mut unit = [0u8];
    rdr.read_exact(&mut unit).unwrap();
    let how_much = rdr.read_u64::<BigEndian>().unwrap();
    Message::Tlease {
        unit: unit[0],
        how_long: how_much,
    }
}
