use serde::Deserialize;
pub mod enumerator;
pub mod source;
pub mod split;

pub const NATS_CONNECTOR: &str = "nats";

#[derive(Clone, Debug, Deserialize)]
pub struct NatsProperties {
    pub stream: String,
    pub nats_address: String,
    pub consumer_name: String,
    pub delivery_subject: String,
    pub delivery_group: String,
}
