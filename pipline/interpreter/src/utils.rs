use std::io::Read;
use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonDefaultErrorKind;
use common_rs::exec::interfaces::pair::PairValueEnum;
use curl::easy::Easy;
use mypip_types::interface::GlobalLayout;

pub(crate) trait ConvertInterpreterParam<T : 'static> {
    fn convert(&self, param : &'_ PairValueEnum) -> Result<T, CommonError>;
}

pub(crate) trait ConvertPairValue<T : 'static> {
    fn convert(&self, param : &'_ T) -> Result<PairValueEnum, CommonError>;
}

pub(crate) fn exec_pair_conn<T : 'static, R :'static>(global : &'static dyn GlobalLayout,
                                                      conn_name : &'_ str,
                                                      query : &'_ str,
                                                      param : T,
                                                      script_converter : impl ConvertInterpreterParam<R>,
                                                      pair_converter : impl ConvertPairValue<T>) -> Result<R, CommonError> {
    let real_args = pair_converter.convert(&param).map_err(|e| {
       CommonError::extend(&CommonDefaultErrorKind::Etc, "pair converter failed", e)
    })?;

    let pool_get_ret = unsafe {
        global.get_exec_pool(conn_name.into())
    }.map_err(|e| {
        CommonError::extend(&CommonDefaultErrorKind::Etc, "get exec pool failed", e)
    })?;

    let mut item = pool_get_ret.get_owned(()).map_err(|e| {
        CommonError::extend(&CommonDefaultErrorKind::Etc, "get exec pool item failed", e)
    })?;

    let conn =item.get_value();

    let conn_ret = conn.execute_pair(query, &real_args);

    let conn_data = if conn_ret.is_err() {
        item.dispose();
        return CommonError::extend(&CommonDefaultErrorKind::ExecuteFail,
                                   "execute failed", conn_ret.err().unwrap()).to_result();
    } else {
        item.restoration();
        Ok(conn_ret.unwrap())
    }?;

    script_converter.convert(&conn_data).map_err(|e| {
        CommonError::extend(&CommonDefaultErrorKind::Etc, "script converter failed", e)
    })
}

pub(crate) fn exec_http_conn(url : &'_ str, method : &'_ str, header : &'_ [String], body : String) -> Result<Vec<u8>, CommonError> {
    let mut easy = Easy::new();

    easy.url(url).map_err(|e| {
        CommonError::new(&CommonDefaultErrorKind::Etc, format!("curl easy url init failed :{}", e))
    })?;

    let curl_header = header.iter().fold(curl::easy::List::new(), |mut acc,x| {
        acc.append(x).expect("libcurl header list append is panic");
        acc
    });

    easy.http_headers(curl_header).map_err(|e| {
        CommonError::new(&CommonDefaultErrorKind::Etc, format!("curl easy http header init failed :{}", e))
    })?;
    
    let mut response_buffer = Vec::with_capacity(1024);

    let resp_ret = match method {
        "POST" => {
            easy.post(true).map_err(|e| {
                CommonError::new(&CommonDefaultErrorKind::Etc, format!("http method post init failed : {}", e))
            })?;

            easy.post_field_size(body.len() as u64).unwrap();
            
            let mut transfer = easy.transfer();
            
            transfer.read_function(|buf| {
                Ok(body.as_bytes().read(buf).unwrap_or(0))
            }).map_err(|e| {
                CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, format!("http method post read init failed : {}", e))
            })?;
            
            transfer.write_function(|buf| {
                response_buffer.extend_from_slice(buf);
                Ok(buf.len())
            }).map_err(|e| {
                CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, format!("http method post write init failed : {}", e))
            })?;
            
            transfer.perform().map_err(|e| {
                CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, format!("http method post response failed : {}", e))
            })?;
        },
        "PUT" => {
            easy.put(true).map_err(|e| {
                CommonError::new(&CommonDefaultErrorKind::Etc, format!("http method put init failed : {}", e))
            })?;
            let mut transfer = easy.transfer();
            
            transfer.read_function(|buf| {
                Ok(body.as_bytes().read(buf).unwrap_or(0))
            }).map_err(|e| {
                CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, format!("http method put read init failed : {}", e))
            })?;

            transfer.perform().map_err(|e| {
                CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, format!("http method put response failed : {}", e))
            })?;
        },
        "GET" => {
            easy.get(true).map_err(|e| {
                CommonError::new(&CommonDefaultErrorKind::Etc, format!("http method get init failed : {}", e))
            })?;

            let mut transfer = easy.transfer();

            transfer.write_function(|buf| {
                response_buffer.extend_from_slice(buf);
                Ok(buf.len())
            }).map_err(|e| {
                CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, format!("http method get write init failed : {}", e))
            })?;

            transfer.perform().map_err(|e| {
                CommonError::new(&CommonDefaultErrorKind::ThirdLibCallFail, format!("http method get response failed : {}", e))
            })?;
        },
        _ => {
            CommonError::new(&CommonDefaultErrorKind::Etc, format!("not support http method {}", method)).to_result()?;
        }
    };
    
    Ok(response_buffer)
}