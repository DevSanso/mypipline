use std::error::Error;

use conn::CommonSqlExecuteResultSet;
use postgres::types::ToSql;
use postgres::types::Type;

use common::err::define as err_def;
use common::err::make_err_msg;
use common::logger::error;
use conn::{CommonSqlConnection, CommonSqlConnectionInfo, CommonValue};

pub struct PostgresConnection {
    client : postgres::Client   
} 

macro_rules! get_pg_data {
    ($row_col : expr, $idx : expr, $origin_t : ty, $common_ident :ident, $common_t :ident) => {
        {
            let opt : Option<$origin_t> = $row_col.get($idx);
            match opt {
                None => CommonValue::Null,
                Some(s) => $common_ident::$common_t(s)
            }
        }
    };
}

impl PostgresConnection {
    fn create_pg_url(username : &'_ str, password : &'_ str, addr : &'_ str, db_name : &'_ str) -> String {
        format!("postgresql://{username}:{password}@{addr}/{db_name}?connect_timeout=60")
    }

    pub(crate) fn new(info : CommonSqlConnectionInfo) -> Result<Self, Box<dyn Error>> {
        let url = Self::create_pg_url(&info.user, &info.password, &info.addr, &info.db_name);

        let conn = match postgres::Client::connect(url.as_str(), postgres::NoTls) {
            Ok(ok) => Ok(ok),
            Err(err) => Err(err_def::connection::GetConnectionFailedError::new(make_err_msg!("{}", err.to_string())))
        }?;

        Ok(PostgresConnection {
            client : conn
        })
    }
}

impl CommonSqlConnection for PostgresConnection {
    fn execute(&mut self, query : &'_ str, param : &'_ [CommonValue]) -> Result<conn::CommonSqlExecuteResultSet, Box<dyn std::error::Error>> {
        let pg_param :  Vec<&(dyn ToSql + Sync)> = param.iter().fold(Vec::new(), |mut acc,x| {
            match x {
                CommonValue::BigInt(i) => acc.push(i),
                CommonValue::Int(i) => acc.push(i),
                CommonValue::Null => acc.push(&Option::<i64>::None),
                CommonValue::Double(f) => acc.push(f),
                CommonValue::Binrary(v) => acc.push(v),
                CommonValue::String(t) => acc.push(t),
                _ => {
                    let err_error = err_def::system::ApiCallError::new(make_err_msg!(
                        "not support type({:?}), return null", x
                    ));
                    error!("{}", err_error);
                    acc.push(&Option::<i64>::None)
                }
            };

            acc
        });

        let rows = match self.client.query(query, pg_param.as_slice()) {
            Ok(ok) => Ok(ok),
            Err(err) =>  Err(err_def::connection::CommandRunError::new(
                make_err_msg!("{}",err.to_string())
            ))
        }?;

        let mut ret = CommonSqlExecuteResultSet::default();

        if rows.len() <= 0 {
            return Ok(ret);
        }
        let mut cols_t = Vec::with_capacity(rows[0].columns().len());

        for col in rows[0].columns() {
            cols_t.push(col.type_());
            ret.cols_name.push(col.name().to_string());
        }

        for row in &rows {
            let mut col_data = Vec::with_capacity(cols_t.len());

            for col_idx in 0..cols_t.len() {
                let d = match cols_t[col_idx] {
                    &Type::BOOL => Ok(get_pg_data!(row, col_idx, bool, CommonValue, Bool)),
                    &Type::CHAR | &Type::VARCHAR | &Type::TEXT => Ok(get_pg_data!(row, col_idx, String, CommonValue, String)),
                    &Type::FLOAT4 | &Type::FLOAT8 | &Type::NUMERIC => Ok(get_pg_data!(row, col_idx, f64, CommonValue, Double)),
                    &Type::INT2 | &Type::INT4 =>Ok(get_pg_data!(row, col_idx, i32, CommonValue, Int)),
                    &Type::INT8 => Ok(get_pg_data!(row, col_idx, i64, CommonValue, BigInt)),
                    &Type::BYTEA => Ok(get_pg_data!(row, col_idx, Vec<u8>, CommonValue, Binrary)),
                    _ => {
                        Err(err_def::connection::ResponseScanError::new(
                            make_err_msg!("not support this type({}), return NULL", cols_t[col_idx])
                        ))
                    }
                }?;

                col_data.push(d);
            }
            ret.cols_data.push(col_data);
        }

        Ok(ret)
    }

    fn get_current_time(&mut self) -> Result<std::time::Duration, Box<dyn Error>> {
        let ret = self.execute("SELECT EXTRACT(EPOCH FROM NOW())::bigint AS unix_timestamp;", &[])?;

        if ret.cols_data.len() <= 0 && ret.cols_data[0].len() <= 0 {
            return Err(err_def::connection::ResponseScanError::new(make_err_msg!("not exists now return data")));
        }

        let data = match ret.cols_data[0][0] {
            CommonValue::BigInt(bi) => bi,
            CommonValue::Int(i) => i as i64,
            _ => 0
        };

        Ok(std::time::Duration::from_secs(data as u64))
    }
}