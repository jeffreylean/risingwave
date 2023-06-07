use anyhow::{anyhow, bail};
use async_nats::jetstream::{self, consumer};
use async_nats::Error;
use async_trait::async_trait;

use crate::source::nats::split::NatsSplit;
use crate::source::nats::NatsProperties;
use crate::source::SplitEnumerator;

pub struct NatsSplitEnumerator {
    stream: String,
}

#[async_trait]
impl SplitEnumerator for NatsSplitEnumerator {
    type Properties = NatsProperties;
    type Split = NatsSplit;

    async fn new(properties: NatsProperties) -> anyhow::Result<Self> {
        let nats_address = properties.nats_address.clone();
        let client = async_nats::connect(nats_address).await?;
        let js = jetstream::new(client.clone());

        let stream = js
            .get_stream(properties.stream.clone())
            .await
            .map_err(|e| anyhow!(e))?;
        let con: Result<consumer::PushConsumer, Error> =
            stream.get_consumer(&properties.consumer_name).await;
        if let Err(e) = con {
            bail!("Error seeking consumer {}", e)
        }

        Ok(Self {
            stream: properties.stream,
        })
    }

    async fn list_splits(&mut self) -> anyhow::Result<Vec<NatsSplit>> {
        // Support single split first
        let split = vec![NatsSplit {
            stream: self.stream.clone(),
        }];
        Ok(split)
    }
}
