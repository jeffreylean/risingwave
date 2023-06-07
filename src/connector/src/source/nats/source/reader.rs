use anyhow::ensure;
use async_nats::Subscriber;
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use futures_async_stream::try_stream;

use crate::impl_common_split_reader_logic;
use crate::parser::ParserConfig;
use crate::source::base::SplitMetaData;
use crate::source::nats::NatsProperties;
use crate::source::{
    BoxSourceWithStateStream, Column, SourceContextRef, SourceMessage, SplitId, SplitImpl,
    SplitReader,
};

impl_common_split_reader_logic!(NatsSplitReader, NatsProperties);

pub struct NatsSplitReader {
    subscription: Subscriber,
    split_id: SplitId,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
}

impl NatsSplitReader {
    #[try_stream(boxed, ok = Vec<SourceMessage>, error = anyhow::Error)]
    async fn into_data_stream(mut self) {
        while let Some(message) = self.subscription.next().await {
            print!("{:?}", message);
        }
    }
}

#[async_trait]
impl SplitReader for NatsSplitReader {
    type Properties = NatsProperties;

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
        let subscription = client
            .queue_subscribe(properties.delivery_subject, properties.delivery_group)
            .await?;

        Ok(Self {
            subscription,
            parser_config,
            source_ctx,
            split_id: split.id(),
        })
    }

    fn into_stream(self) -> BoxSourceWithStateStream {
        self.into_chunk_stream()
    }
}
