macro_rules! cast_response_data {
    ($idx:expr, $typ_vec:expr, $row_cols :expr) => {
        {
            use scylla::frame::response::result::{ColumnType,CqlValue};

            let get_row_data = || -> Result<&'_ CqlValue, Box<dyn Error>> {
                match $idx {
                    0 => Ok(&$row_cols.0),
                    1 => Ok(&$row_cols.1),
                    2 => Ok(&$row_cols.2),
                    3 => Ok(&$row_cols.3),
                    4 => Ok(&$row_cols.4),
                    5 => Ok(&$row_cols.5),
                    6 => Ok(&$row_cols.6),
                    7 => Ok(&$row_cols.7),
                    8 => Ok(&$row_cols.8),
                    9 => Ok(&$row_cols.9),
                    10 => Ok(&$row_cols.10),
                    11 => Ok(&$row_cols.11),
                    12 => Ok(&$row_cols.12),
                    13 => Ok(&$row_cols.13),
                    14 => Ok(&$row_cols.14),
                    _ =>  Err(err_def::system::OverflowSizeError::new(make_err_msg!(
                        "{}", "cast_response_data - get_row_data - Reponse Max Size over"
                    )))
                }
            };

            let cast_cql_val_to_comm_int_value = |t : &'_ ColumnType, cql_value : &'_ CqlValue| {
                match t {
                    ColumnType::Int => {
                        let opt = cql_value.as_int();
                        if opt.is_none() {
                            CommonValue::Null
                        }else {
                            CommonValue::Int(opt.unwrap())
                        }
                    },
                    ColumnType::TinyInt => {
                        let opt = cql_value.as_tinyint();
                        if opt.is_none() {
                            CommonValue::Null
                        }else {
                            CommonValue::Int(opt.unwrap() as i32)
                        }
                    },
                    _ => CommonValue::Null
                }
            };
            let cast_cql_val_to_comm_bigint_value = |cql_value : &'_ CqlValue| {
                let opt = cql_value.as_bigint();
                if opt.is_none() {
                    CommonValue::Null
                }else {
                    CommonValue::BigInt(opt.unwrap())
                }
            };
            let cast_cql_val_to_comm_float_value = |cql_value : &'_ CqlValue| {
                let opt = cql_value.as_float();
                if opt.is_none() {
                    CommonValue::Null
                }else {
                    CommonValue::Double(opt.unwrap() as f64)
                }
            };
            let cast_cql_val_to_comm_text_value = |cql_value : &'_ CqlValue| {
                let opt = cql_value.as_text();
                if opt.is_none() {
                    CommonValue::Null
                }else {
                    CommonValue::String(opt.unwrap().clone())
                }
            };
            let cast_cql_val_to_comm_blob_value = |cql_value : &'_ CqlValue| {
                let opt = cql_value.as_blob();
                if opt.is_none() {
                    CommonValue::Null
                }else {
                    CommonValue::Binrary(opt.unwrap().clone())
                }
            };
            let cast_cql_val_to_comm_bool_value = |cql_value : &'_ CqlValue| {
                let opt = cql_value.as_boolean();
                if opt.is_none() {
                    CommonValue::Null
                }else {
                    CommonValue::Bool(opt.unwrap())
                }
            };       

            let cql_value = get_row_data()?;

            let d = match $typ_vec[$idx] {
               ColumnType::Int | ColumnType::TinyInt => cast_cql_val_to_comm_int_value($typ_vec[$idx],cql_value),
               ColumnType::BigInt => cast_cql_val_to_comm_bigint_value(cql_value),
               ColumnType::Boolean => cast_cql_val_to_comm_bool_value(cql_value),
               ColumnType::Blob => cast_cql_val_to_comm_blob_value(cql_value),
               ColumnType::Text => cast_cql_val_to_comm_text_value(cql_value),
               ColumnType::Float => cast_cql_val_to_comm_float_value(cql_value),
               
               _ => return Err(err_def::connection::ResponseScanError::new(
                    make_err_msg!("copy_reponse_data - can't cast data type:{:?} idx:{}", $typ_vec[$idx], $idx)
               ))
            };
            d
        }
    };
}

pub(super) use cast_response_data;