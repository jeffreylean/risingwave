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

/// Mapping from sql name to system locale groups.
/// Reference: [`https://www.postgresql.org/docs/current/catalog-pg-collation.html`].
pub const PG_COLLATION: BuiltinTable = BuiltinTable {
    name: "pg_collation",
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Int32, "oid"),
        (DataType::Varchar, "collname"),
        (DataType::Int32, "collnamespace"),
        (DataType::Int32, "collowner"),
        (DataType::Int32, "collprovider"),
        (DataType::Boolean, "collisdeterministic"),
        (DataType::Int32, "collencoding"),
        (DataType::Varchar, "collcollate"),
        (DataType::Varchar, "collctype"),
        (DataType::Varchar, "colliculocale"),
        (DataType::Varchar, "collversion"),
    ],
    pk: &[0],
};

impl SysCatalogReaderImpl {
    pub fn read_collation_info(&self) -> Result<Vec<OwnedRow>> {
        Ok(vec![])
    }
}
