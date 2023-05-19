use std::thread;

use async_nats::jetstream::{self, consumer, context};
use async_nats::Subscriber;
use futures::StreamExt;
use futures_async_stream::try_stream;

use crate::parser::ParserConfig;
use crate::source::google_pubsub::TaggedReceivedMessage;
use crate::source::nats::NatsProperties;
use crate::source::{
    Column, SourceContext, SourceContextRef, SourceMessage, SplitId, SplitImpl, SplitReader,
};

pub struct NatsSplitReader {
    subscription: Subscriber,
    split_id: SplitId,
    parser_config: ParserConfig,
    source_ctx: SourceContext,
}

impl NatsSplitReader {
    #[try_stream(boxed, ok = Vec<SourceMessage>, error = anyhow::Error)]
    async fn into_data_stream(self) {
        while let Some(message) = self.subscription.next().await {}
        print!("{:}", message);
    }
}

impl SplitReader for NatsSplitReader {
    async fn new(
        properties: NatsProperties,
        splits: Vec<SplitImpl>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> anyhow::Result<Self> {
        ensure!(splits.len() == 1, "NATS reader only support single split");
        let split = splits.into_iter().next().unwrap().into_nats().unwrap();

        let client = async_nats::connect(properties.nats_address).await?;
        let mut subscription = client
            .queue_subscribe(properties.delivery_subject, properties.delivery_group)
            .await?;

        Ok(Self {
            subscription,
            parser_config,
            source_ctx,
            split_id: split.id(),
        })
    }
}
