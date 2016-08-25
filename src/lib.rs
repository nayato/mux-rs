extern crate byteorder;

mod transport;

type Path = String;

struct Dentry {
    prefix: String,
    dst: String,
}

struct Dtab {
    dentries: Vec<Dentry>,
}

impl Dtab {
    fn len(&self) -> usize {
        self.dentries.len()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
