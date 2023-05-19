use anyhow::bail;
use async_nats::jetstream::{self, consumer, context};

use crate::source::nats::split::NatsSplit;
use crate::source::nats::NatsProperties;
use crate::source::SplitEnumerator;

pub struct NatsSplitEnumerator {
    stream: String,
}

impl SplitEnumerator for NatsSplitEnumerator {
    async fn new(properties: NatsProperties) -> anyhow::Result<Self> {
        let nats_address = properties.nats_address.clone();
        let client = async_nats::connect(nats_address).await?;
        let js = jetstream::new(client.clone());

        let stream = js.get_stream(properties.stream).await?;
        if let Err(e) = stream.get_consumer(&properties.consumer_name).await {
            bail!("error seeking consumer {}", e)
        };

        Ok(Self {
            stream: properties.stream,
        })
    }

    async fn list_splits(&mut self) -> anyhow::Result<Vec<NatsSplit>> {
        // Support single split first
        let split = vec![NatsSplit {
            stream: self.stream,
        }];
        Ok(split)
    }
}
