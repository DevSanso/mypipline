use std::error::Error;

use scylla::frame::response::result::{ColumnType,CqlValue};
use scylla::deserialize::result::TypedRowIterator;
use scylla::QueryRowsResult;

use crate::types as res_type;
use common::err::define as err_def;
use common::err::make_err_msg;
use conn::CommonValue;
use conn::CommonSqlExecuteResultSet;

pub(super) struct ScyllaFetcher<'a> {
    fetch : &'a QueryRowsResult,
    cols_desc : &'a Vec<&'a ColumnType<'a>>
}

impl<'a> ScyllaFetcher<'a> {
    pub fn new(fetch : &'a QueryRowsResult, cols_desc : &'a Vec<&'a ColumnType<'a>>) -> Self {
        ScyllaFetcher {
            fetch,
            cols_desc
        }
    }

    #[inline]
    fn cast_cql_val_to_comm_int_value(t : &'_ ColumnType, cql_value : &'_ CqlValue) -> CommonValue {
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
    }

    #[inline]
    fn cast_cql_val_to_comm_bigint_value(cql_value : &'_ CqlValue) -> CommonValue {
        let opt = cql_value.as_bigint();
        if opt.is_none() {
            CommonValue::Null
        }else {
            CommonValue::BigInt(opt.unwrap())
        }
    }

    #[inline]
    fn cast_cql_val_to_comm_float_value(cql_value : &'_ CqlValue) -> CommonValue {
        let opt = cql_value.as_float();
        if opt.is_none() {
            CommonValue::Null
        }else {
            CommonValue::Float(opt.unwrap())
        }
    }

    #[inline]
    fn cast_cql_val_to_comm_double_value(cql_value : &'_ CqlValue) -> CommonValue {
        let opt = cql_value.as_double();
        if opt.is_none() {
            CommonValue::Null
        }else {
            CommonValue::Double(opt.unwrap())
        }
    }

    #[inline]
    fn cast_cql_val_to_comm_text_value(cql_value : &'_ CqlValue) -> CommonValue {
        let opt = cql_value.as_text();
        if opt.is_none() {
            CommonValue::Null
        }else {
            CommonValue::String(opt.unwrap().clone())
        }
    }

    #[inline]
    fn cast_cql_val_to_comm_blob_value(cql_value : &'_ CqlValue) -> CommonValue {
        let opt = cql_value.as_blob();
        if opt.is_none() {
            CommonValue::Null
        }else {
            CommonValue::Binrary(opt.unwrap().clone())
        }
    }

    #[inline]
    fn cast_cql_val_to_comm_bool_value(cql_value : &'_ CqlValue) -> CommonValue {
        let opt = cql_value.as_boolean();
        if opt.is_none() {
            CommonValue::Null
        }else {
            CommonValue::Bool(opt.unwrap())
        }
    }

    fn cast_data(t : &'_ ColumnType, cql_value : &'_ CqlValue) -> Result<CommonValue, Box<dyn Error>> {
        let d = match t {
            ColumnType::Int | ColumnType::TinyInt => Self::cast_cql_val_to_comm_int_value(t,cql_value),
            ColumnType::BigInt => Self::cast_cql_val_to_comm_bigint_value(cql_value),
            ColumnType::Boolean => Self::cast_cql_val_to_comm_bool_value(cql_value),
            ColumnType::Blob => Self::cast_cql_val_to_comm_blob_value(cql_value),
            ColumnType::Text => Self::cast_cql_val_to_comm_text_value(cql_value),
            ColumnType::Float => Self::cast_cql_val_to_comm_float_value(cql_value),
            ColumnType::Double => Self::cast_cql_val_to_comm_double_value(cql_value),
            
            _ => return Err(err_def::connection::ResponseScanError::new(
                 make_err_msg!("copy_reponse_data - can't cast data type:{:?}", t), None
            ))
         };
        Ok(d)
    }

    fn fetch_iter<T : scylla::deserialize::DeserializeRow<'a,'a>> (query_ret : &'a QueryRowsResult) -> Result<TypedRowIterator<'a, 'a, T>, Box<dyn Error>> {
        match query_ret.rows::<T>(){
            Ok(ok) => Ok(ok),
            Err(err) => Err(err_def::connection::ResponseScanError::new(make_err_msg!("{}", err), None))
        }
    }

    fn copy_response1(&self, row : res_type::Response1) -> Result<Vec<CommonValue>, Box<dyn Error>> {
        let mut data = Vec::with_capacity(1);
        let val = match Self::cast_data(self.cols_desc[0], &row.0) {
            Ok(ok) => ok,
            Err(e) => return Err(err_def::connection::ResponseScanError::new(make_err_msg!(""), Some(e)))
        };
        data.push(val);

        Ok(data)
    }

    fn copy_response2(&self, row : res_type::Response2) -> Result<Vec<CommonValue>, Box<dyn Error>> {
        let mut data = Vec::with_capacity(2);
        for idx in 0..2 {
            let cal_val = match idx {
                0 => &row.0,
                _ => &row.1
            };

            let val = match Self::cast_data(self.cols_desc[idx], cal_val) {
                Ok(ok) => ok,
                Err(e) => return Err(err_def::connection::ResponseScanError::new(make_err_msg!(""), Some(e)))
            };
            data.push(val);
        }

        Ok(data)
    }

    fn copy_response3(&self, row : res_type::Response3) -> Result<Vec<CommonValue>, Box<dyn Error>> {
        let mut data = Vec::with_capacity(3);
        for idx in 0..3 {
            let cal_val = match idx {
                0 => &row.0,
                1 => &row.1,
                _ => &row.2
            };

            let val = match Self::cast_data(self.cols_desc[idx], cal_val) {
                Ok(ok) => ok,
                Err(e) => return Err(err_def::connection::ResponseScanError::new(make_err_msg!(""), Some(e)))
            };
            data.push(val);
        }

        Ok(data)
    }

    fn copy_response4(&self, row : res_type::Response4) -> Result<Vec<CommonValue>, Box<dyn Error>> {
        let mut data = Vec::with_capacity(3);
        for idx in 0..4 {
            let cal_val = match idx {
                0 => &row.0,
                1 => &row.1,
                2 => &row.2,
                _ => &row.3
            };

            let val = match Self::cast_data(self.cols_desc[idx], cal_val) {
                Ok(ok) => ok,
                Err(e) => return Err(err_def::connection::ResponseScanError::new(make_err_msg!(""), Some(e)))
            };
            data.push(val);
        }

        Ok(data)
    }

    pub fn fetch(&mut self, output : &mut CommonSqlExecuteResultSet) -> Result<(), Box<dyn Error>> {
        let col_len = self.cols_desc.len();

        if col_len <= 0 || self.fetch.rows_num() <= 0 {
            return Ok(());
        }

        match col_len {
            1 => {
                let mut fetch_data_iter = match Self::fetch_iter::<res_type::Response1>(&self.fetch) {
                    Ok(ok) => Ok(ok),
                    Err(e) => Err(err_def::connection::CommandRunError::new(make_err_msg!(""), Some(e)))
                }?;

                #[allow(irrefutable_let_patterns)]
                while let row_scan_ret = fetch_data_iter.next().transpose() {
                    let row_opt = match row_scan_ret {
                        Ok(ok) => Ok(ok),
                        Err(err) => Err(err_def::connection::ResponseScanError::new(make_err_msg!("{}", err), None))
                    }?;
        
                    let row = match row_opt {
                        Some(s) => s,
                        None => break
                    };
                    let data = self.copy_response1(row).map_err(|e| {
                        err_def::connection::ResponseScanError::new(make_err_msg!(""), Some(e))
                    })?;
                    
                    output.cols_data.push(data);
                }
            },
            2 => {
                let mut fetch_data_iter = match Self::fetch_iter::<res_type::Response2>(&self.fetch) {
                    Ok(ok) => Ok(ok),
                    Err(e) => Err(err_def::connection::CommandRunError::new(make_err_msg!(""), Some(e)))
                }?;

                #[allow(irrefutable_let_patterns)]
                while let row_scan_ret = fetch_data_iter.next().transpose() {
                    let row_opt = match row_scan_ret {
                        Ok(ok) => Ok(ok),
                        Err(err) => Err(err_def::connection::ResponseScanError::new(make_err_msg!("{}", err), None))
                    }?;
        
                    let row = match row_opt {
                        Some(s) => s,
                        None => break
                    };
                    let data = self.copy_response2(row).map_err(|e| {
                        err_def::connection::ResponseScanError::new(make_err_msg!(""), Some(e))
                    })?;
                    
                    output.cols_data.push(data);
                }
            },
            3 => {
                let mut fetch_data_iter = match Self::fetch_iter::<res_type::Response3>(&self.fetch) {
                    Ok(ok) => Ok(ok),
                    Err(e) => Err(err_def::connection::CommandRunError::new(make_err_msg!(""), Some(e)))
                }?;

                #[allow(irrefutable_let_patterns)]
                while let row_scan_ret = fetch_data_iter.next().transpose() {
                    let row_opt = match row_scan_ret {
                        Ok(ok) => Ok(ok),
                        Err(err) => Err(err_def::connection::ResponseScanError::new(make_err_msg!("{}", err), None))
                    }?;
        
                    let row = match row_opt {
                        Some(s) => s,
                        None => break
                    };
                    let data = self.copy_response3(row).map_err(|e| {
                        err_def::connection::ResponseScanError::new(make_err_msg!(""), Some(e))
                    })?;
                    
                    output.cols_data.push(data);
                }
            },
            _ => {
                let mut fetch_data_iter = match Self::fetch_iter::<res_type::Response4>(&self.fetch) {
                    Ok(ok) => Ok(ok),
                    Err(e) => Err(err_def::connection::CommandRunError::new(make_err_msg!(""), Some(e)))
                }?;

                #[allow(irrefutable_let_patterns)]
                while let row_scan_ret = fetch_data_iter.next().transpose() {
                    let row_opt = match row_scan_ret {
                        Ok(ok) => Ok(ok),
                        Err(err) => Err(err_def::connection::ResponseScanError::new(make_err_msg!("{}", err), None))
                    }?;
        
                    let row = match row_opt {
                        Some(s) => s,
                        None => break
                    };
                    let data = self.copy_response4(row).map_err(|e| {
                        err_def::connection::ResponseScanError::new(make_err_msg!(""), Some(e))
                    })?;
                    
                    output.cols_data.push(data);
                }
            }
        }

        Ok(())
    }
}