extern crate byteorder;
extern crate tokio_proto as proto;

#[macro_use]
extern crate log;

mod transport;

type Path = String;

struct Dentry {
    prefix: String,
    dst: String,
}

type Dtab = Vec<Dentry>;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
