extern crate byteorder;

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
