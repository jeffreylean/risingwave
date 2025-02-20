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

use std::sync::Arc;

use itertools::Itertools;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::{PgResponse, StatementType};
use pgwire::types::Row;
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::error::Result;
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{display_comma_separated, ObjectName};

use super::RwPgResponse;
use crate::binder::{Binder, Relation};
use crate::catalog::{CatalogError, IndexCatalog};
use crate::handler::util::col_descs_to_rows;
use crate::handler::HandlerArgs;

pub fn handle_describe(handler_args: HandlerArgs, table_name: ObjectName) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let mut binder = Binder::new_for_system(&session);
    let relation = binder.bind_relation_by_name(table_name.clone(), None, false)?;
    // For Source, it doesn't have table catalog so use get source to get column descs.
    let (columns, pk_columns, indices): (Vec<ColumnDesc>, Vec<ColumnDesc>, Vec<Arc<IndexCatalog>>) = {
        let (column_catalogs, pk_column_catalogs, indices) = match relation {
            Relation::Source(s) => {
                let pk_column_catalogs = s
                    .catalog
                    .pk_col_ids
                    .iter()
                    .map(|&column_id| {
                        s.catalog
                            .columns
                            .iter()
                            .filter(|x| x.column_id() == column_id)
                            .exactly_one()
                            .unwrap()
                            .clone()
                    })
                    .collect_vec();
                (s.catalog.columns, pk_column_catalogs, vec![])
            }
            Relation::BaseTable(t) => {
                let pk_column_catalogs = t
                    .table_catalog
                    .pk()
                    .iter()
                    .map(|x| t.table_catalog.columns[x.column_index].clone())
                    .collect_vec();
                (t.table_catalog.columns, pk_column_catalogs, t.table_indexes)
            }
            Relation::SystemTable(t) => {
                let pk_column_catalogs = t
                    .sys_table_catalog
                    .pk
                    .iter()
                    .map(|idx| t.sys_table_catalog.columns[*idx].clone())
                    .collect_vec();
                (
                    t.sys_table_catalog.columns.clone(),
                    pk_column_catalogs,
                    vec![],
                )
            }
            _ => {
                return Err(
                    CatalogError::NotFound("table or source", table_name.to_string()).into(),
                );
            }
        };
        (
            column_catalogs
                .into_iter()
                .filter(|c| !c.is_hidden)
                .map(|c| c.column_desc)
                .collect(),
            pk_column_catalogs
                .into_iter()
                .map(|c| c.column_desc)
                .collect(),
            indices,
        )
    };

    // Convert all column descs to rows
    let mut rows = col_descs_to_rows(columns);

    // Convert primary key to rows
    if !pk_columns.is_empty() {
        rows.push(Row::new(vec![
            Some("primary key".into()),
            Some(
                format!(
                    "{}",
                    display_comma_separated(&pk_columns.into_iter().map(|x| x.name).collect_vec()),
                )
                .into(),
            ),
        ]));
    }

    // Convert all indexes to rows
    rows.extend(indices.iter().map(|index| {
        let index_display = index.display();

        Row::new(vec![
            Some(index.name.clone().into()),
            if index_display.include_columns.is_empty() {
                Some(
                    format!(
                        "index({}) distributed by({})",
                        display_comma_separated(&index_display.index_columns_with_ordering),
                        display_comma_separated(&index_display.distributed_by_columns),
                    )
                    .into(),
                )
            } else {
                Some(
                    format!(
                        "index({}) include({}) distributed by({})",
                        display_comma_separated(&index_display.index_columns_with_ordering),
                        display_comma_separated(&index_display.include_columns),
                        display_comma_separated(&index_display.distributed_by_columns),
                    )
                    .into(),
                )
            },
        ])
    }));

    // TODO: recover the original user statement
    Ok(PgResponse::builder(StatementType::DESCRIBE)
        .values(
            rows.into(),
            vec![
                PgFieldDescriptor::new(
                    "Name".to_owned(),
                    DataType::Varchar.to_oid(),
                    DataType::Varchar.type_len(),
                ),
                PgFieldDescriptor::new(
                    "Type".to_owned(),
                    DataType::Varchar.to_oid(),
                    DataType::Varchar.type_len(),
                ),
            ],
        )
        .into())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ops::Index;

    use futures_async_stream::for_await;

    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_describe_handler() {
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend
            .run_sql("create table t (v1 int, v2 int, v3 int primary key, v4 int);")
            .await
            .unwrap();

        frontend
            .run_sql("create index idx1 on t (v1 DESC, v2);")
            .await
            .unwrap();

        let sql = "describe t";
        let mut pg_response = frontend.run_sql(sql).await.unwrap();

        let mut columns = HashMap::new();
        #[for_await]
        for row_set in pg_response.values_stream() {
            let row_set = row_set.unwrap();
            for row in row_set {
                columns.insert(
                    std::str::from_utf8(row.index(0).as_ref().unwrap())
                        .unwrap()
                        .to_string(),
                    std::str::from_utf8(row.index(1).as_ref().unwrap())
                        .unwrap()
                        .to_string(),
                );
            }
        }

        let expected_columns: HashMap<String, String> = maplit::hashmap! {
            "v1".into() => "integer".into(),
            "v2".into() => "integer".into(),
            "v3".into() => "integer".into(),
            "v4".into() => "integer".into(),
            "primary key".into() => "v3".into(),
            "idx1".into() => "index(v1 DESC, v2 ASC, v3 ASC) include(v4) distributed by(v1, v2)".into(),
        };

        assert_eq!(columns, expected_columns);
    }
}
