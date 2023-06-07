use anyhow::anyhow;
use risingwave_common::types::JsonbVal;
use serde::{Deserialize, Serialize};

use crate::source::base::SplitMetaData;
use crate::source::SplitId;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct NatsSplit {
    pub(crate) stream: String,
}

impl SplitMetaData for NatsSplit {
    fn id(&self) -> SplitId {
        self.stream.to_string().into()
    }

    fn restore_from_json(value: JsonbVal) -> anyhow::Result<Self> {
        serde_json::from_value(value.take()).map_err(|e| anyhow!(e))
    }

    fn encode_to_json(&self) -> JsonbVal {
        serde_json::to_value(self.clone()).unwrap().into()
    }
}

impl NatsSplit {
    pub fn new(stream: String) -> Self {
        NatsSplit { stream }
    }

    pub fn copy_with_offset(&self, _start_offset: String) -> Self {
        self.clone()
    }
}
