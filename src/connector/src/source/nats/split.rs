#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct NatsSplit {
    pub(crate) stream: String,
}

impl NatsSplit {
    pub fn new(stream: String) -> Self {
        NatsSplit { stream }
    }
}
