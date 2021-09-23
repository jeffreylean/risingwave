use crate::error::ErrorCode::{InternalError, ProtobufError};
use crate::error::Result;
use crate::expr::binary_expr::new_binary_expr;
use crate::expr::build_from_proto as expr_build_from_proto;
use crate::expr::unary_expr::new_unary_expr;
use crate::expr::BoxedExpression;
use crate::types::build_from_proto as type_build_from_proto;
use protobuf::Message;
use risingwave_proto::expr::{ExprNode, FunctionCall};

pub fn build_unary_expr(proto: &ExprNode) -> Result<BoxedExpression> {
    // FIXME: Note that now it only contains some other expressions so it may not be general enough.
    let data_type = type_build_from_proto(proto.get_return_type())?;
    let function_call_node =
        FunctionCall::parse_from_bytes(proto.get_body().get_value()).map_err(ProtobufError)?;
    match function_call_node.get_children().get(0) {
        Some(child_expr_node) => {
            let child_expr = expr_build_from_proto(child_expr_node)?;
            Ok(new_unary_expr(proto.get_expr_type(), data_type, child_expr))
        }

        None => Err(InternalError(
            "Type cast expression can only have exactly one child".to_string(),
        )
        .into()),
    }
}

pub fn build_binary_expr(proto: &ExprNode) -> Result<BoxedExpression> {
    // FIXME: Note that now it only contains some other expressions so it may not be general enough.
    let data_type = type_build_from_proto(proto.get_return_type())?;
    let function_call_node =
        FunctionCall::parse_from_bytes(proto.get_body().get_value()).map_err(ProtobufError)?;
    let children = function_call_node.get_children();
    ensure!(children.len() == 2);
    let left_expr = expr_build_from_proto(&children[0])?;
    let right_expr = expr_build_from_proto(&children[1])?;
    Ok(new_binary_expr(
        proto.get_expr_type(),
        data_type,
        left_expr,
        right_expr,
    ))
}
