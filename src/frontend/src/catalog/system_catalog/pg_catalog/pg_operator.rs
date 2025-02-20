// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_common::catalog::PG_CATALOG_SCHEMA_NAME;
use risingwave_common::error::Result;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::DataType;

use crate::catalog::system_catalog::{BuiltinTable, SysCatalogReaderImpl};

/// The catalog `pg_operator` stores operator info.
/// Reference: [`https://www.postgresql.org/docs/current/catalog-pg-operator.html`]
pub const PG_OPERATOR: BuiltinTable = BuiltinTable {
    name: "pg_operator",
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Int32, "oid"),
        (DataType::Varchar, "oprname"),
        (DataType::Int32, "oprnamespace"),
        (DataType::Int32, "oprowner"),
        (DataType::Varchar, "oprkind"),
        (DataType::Boolean, "oprcanmerge"),
        (DataType::Boolean, "oprcanhash"),
        (DataType::Int32, "oprleft"),
        (DataType::Int32, "oprright"),
        (DataType::Int32, "oprresult"),
        (DataType::Int32, "oprcom"),
        (DataType::Int32, "oprnegate"),
        (DataType::Int32, "oprcode"),
        (DataType::Int32, "oprrest"),
        (DataType::Int32, "oprjoin"),
    ],
    pk: &[0],
};

impl SysCatalogReaderImpl {
    pub fn read_operator_info(&self) -> Result<Vec<OwnedRow>> {
        Ok(vec![])
    }
}
