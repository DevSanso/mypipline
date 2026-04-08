use common_rs::c_err::CommonError;
use common_rs::c_err::gen::CommonDefaultErrorKind;
use common_rs::exec::interfaces::pair::PairValueEnum;

macro_rules! plan_toml_select_query {
     ($bind_expr:expr) => {
        concat!(
r#"
select
    name,
    toml_data
from
	mypip_plan_toml
where
   "enable" = true
   AND identifier = "#, $bind_expr,
        )
    };
}
macro_rules! plan_select_query {
    ($bind_expr:expr) => {
        concat!(
            r#"
SELECT
    p.name                  AS plan_name,
    p.type_name             AS type,
    p.interval_connection   as interval_connection,
    p.interval_second      AS interval_second,
    pc.id                   AS chain_id,
    pc.next_chain_id        AS chain_next_id,
    pc.connection           AS chain_connection,
    pc.query               AS chain_query,
    m.mapping_type         AS mapping_type,
    m.ranking              AS mapping_ranking,
    pa.data                 AS arg_data,
    pa.idx                  AS arg_idx,
    pb.bind_id             AS bind_id,
    pb.key                  AS bind_key,
    pb.row                  AS bind_row,
    pb.idx                  AS bind_idx,
    ps.lang                 AS script_lang,
    ps."file"                 AS script_file
FROM mypip_plan p
LEFT JOIN mypip_plan_chain              pc ON pc.plan_id         = p.id
LEFT JOIN mypip_plan_chain_mapping      m  ON m.chain_id         = pc.id
LEFT JOIN mypip_plan_chain_args         pa ON pa.chain_id        = pc.id
                                          AND pa.id              = m.args_or_bind_id
                                          AND m.mapping_type     = 'args'
LEFT JOIN mypip_plan_chain_bind_param   pb ON pb.chain_id        = pc.id
                                          AND pb.id              = m.args_or_bind_id
                                          AND m.mapping_type     = 'bind'
LEFT JOIN mypip_plan_script             ps ON ps.plan_id         = p.id
where
	p."enable" = true
	AND p.identifier = "#, $bind_expr,

r#"
 ORDER BY
    p.name,
    pc.next_chain_id,
    m.ranking,
    pa.idx,
    pb.idx;
"#
        )
    };
}

macro_rules! conn_select_query {
    ($bind_expr:expr) => {
        concat!(
r#"
select
	max_size,
	name,
	conn_type,
	conn_name,
	conn_user,
	conn_addr,
	conn_passwd,
	conn_timeout,
	odbc_driver,
	odbc_current_time_query,
	odbc_current_time_col_name
from
	mypip_connection_info
where
   identifier = "#, $bind_expr,
        )
    };
}

macro_rules! script_data_select_query {
    ($bind_expr:expr) => {
        concat!(
r#"
select
	script_file,
	script_data
from
	mypip_plan_script_data
where
	identifier = "#, $bind_expr,
        )
    };
}
macro_rules! vec_if_same_len {
    ($($vec:expr),+) => {{
        let lengths = vec![$($vec.len()),+];
        let first = lengths[0];
        if !lengths.iter().all(|&l| l == first) {
            true
        } else {
            false
        }
    }};
}

macro_rules! get_col_ref {
    ($col:expr, $data:expr, str) => {
        $crate::pair_db_loader::utils::get_col_ref_str($col, $data).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ParsingFail, $col, e)
        })
    };

    ($col:expr, $data:expr, i64) => {
        $crate::pair_db_loader::utils::get_col_ref_i64($col, $data).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ParsingFail, $col, e)
        })
    };

    ($col:expr, $data:expr, i32) => {
        $crate::pair_db_loader::utils::get_col_ref_i32($col, $data).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ParsingFail, $col, e)
        })
    };

    ($col:expr, $data:expr, str, null) => {
        $crate::pair_db_loader::utils::get_col_ref_str_null($col, $data).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ParsingFail, $col, e)
        })
    };

    ($col:expr, $data:expr, i64, null) => {
        $crate::pair_db_loader::utils::get_col_ref_i64_null($col, $data).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ParsingFail, $col, e)
        })
    };

    ($col:expr, $data:expr, i32, null) => {
        $crate::pair_db_loader::utils::get_col_ref_i32_null($col, $data).map_err(|e| {
            CommonError::extend(&CommonDefaultErrorKind::ParsingFail, $col, e)
        })
    };
}

pub(crate) fn get_col_ref_str<'a>(col:  &'a str, data :  &'a PairValueEnum) -> Result<Vec<&'a String>, CommonError> {
    if let PairValueEnum::Map(map) = data {
        if let Some(PairValueEnum::Array(vec)) = map.get(col) {
            let mut res = Vec::with_capacity(vec.len());
            for r in vec {
                if let PairValueEnum::String(s) = r {
                    res.push(s)
                } else {
                    return CommonError::new(&CommonDefaultErrorKind::NotMatchArgs, format!("str != {:?}", r)).to_result()
                }
            }
            Ok(res)
        } else {
            CommonError::new(&CommonDefaultErrorKind::NotMatchArgs, format!("array != {:?}", data)).to_result()
        }
    } else {
        CommonError::new(&CommonDefaultErrorKind::NotMatchArgs, format!("map != {:?}", data)).to_result()
    }
}

pub(crate) fn get_col_ref_i32<'a>(col:  &'a str, data :  &'a PairValueEnum) -> Result<Vec<&'a i32>, CommonError> {
    if let PairValueEnum::Map(map) = data {
        if let Some(PairValueEnum::Array(vec)) = map.get(col) {
            let mut res = Vec::with_capacity(vec.len());
            for r in vec {
                if let PairValueEnum::Int(s) = r {
                    res.push(s)
                } else {
                    return CommonError::new(&CommonDefaultErrorKind::NotMatchArgs, format!("str != {:?}", r)).to_result()
                }
            }
            Ok(res)
        } else {
            CommonError::new(&CommonDefaultErrorKind::NotMatchArgs, format!("i32 != {:?}", data)).to_result()
        }
    } else {
        CommonError::new(&CommonDefaultErrorKind::NotMatchArgs, format!("map != {:?}", data)).to_result()
    }
}

pub(crate) fn get_col_ref_i64<'a>(col:  &'a str, data :  &'a PairValueEnum) -> Result<Vec<&'a i64>, CommonError> {
    if let PairValueEnum::Map(map) = data {
        if let Some(PairValueEnum::Array(vec)) = map.get(col) {
            let mut res = Vec::with_capacity(vec.len());
            for r in vec {
                if let PairValueEnum::BigInt(s) = r {
                    res.push(s)
                } else {
                    return CommonError::new(&CommonDefaultErrorKind::NotMatchArgs, format!("i64 != {:?}", r)).to_result()
                }
            }
            Ok(res)
        } else {
            CommonError::new(&CommonDefaultErrorKind::NotMatchArgs, format!("array != {:?}", data)).to_result()
        }
    } else {
        CommonError::new(&CommonDefaultErrorKind::NotMatchArgs, format!("map != {:?}", data)).to_result()
    }
}

pub(crate) fn get_col_ref_str_null<'a>(col:  &'a str, data :  &'a PairValueEnum) -> Result<Vec<Option<&'a String>>, CommonError> {
    if let PairValueEnum::Map(map) = data {
        if let Some(PairValueEnum::Array(vec)) = map.get(col) {
            let mut res = Vec::with_capacity(vec.len());
            for r in vec {
                match r {
                    PairValueEnum::String(s) => {
                        res.push(Some(s))
                    },
                    PairValueEnum::Null => {
                        res.push(None)
                    },
                    _ => {
                        return CommonError::new(&CommonDefaultErrorKind::NotMatchArgs, format!("str != {:?}", r)).to_result()
                    }
                }
            }
            Ok(res)
        } else {
            CommonError::new(&CommonDefaultErrorKind::NotMatchArgs, format!("array != {:?}", data)).to_result()
        }
    } else {
        CommonError::new(&CommonDefaultErrorKind::NotMatchArgs, format!("map != {:?}", data)).to_result()
    }
}

pub(crate) fn get_col_ref_i32_null<'a>(col:  &'a str, data :  &'a PairValueEnum) -> Result<Vec<Option<&'a i32>>, CommonError> {
    if let PairValueEnum::Map(map) = data {
        if let Some(PairValueEnum::Array(vec)) = map.get(col) {
            let mut res = Vec::with_capacity(vec.len());
            for r in vec {
                match r {
                    PairValueEnum::Int(s) => {
                        res.push(Some(s))
                    },
                    PairValueEnum::Null => {
                        res.push(None)
                    },
                    _ => {
                        return CommonError::new(&CommonDefaultErrorKind::NotMatchArgs, format!("str != {:?}", r)).to_result()
                    }
                }
            }
            Ok(res)
        } else {
            CommonError::new(&CommonDefaultErrorKind::NotMatchArgs, format!("i32 != {:?}", data)).to_result()
        }
    } else {
        CommonError::new(&CommonDefaultErrorKind::NotMatchArgs, format!("map != {:?}", data)).to_result()
    }
}

pub(crate) fn get_col_ref_i64_null<'a>(col:  &'a str, data :  &'a PairValueEnum) -> Result<Vec<Option<&'a i64>>, CommonError> {
    if let PairValueEnum::Map(map) = data {
        if let Some(PairValueEnum::Array(vec)) = map.get(col) {
            let mut res = Vec::with_capacity(vec.len());
            for r in vec {
                match r {
                    PairValueEnum::BigInt(s) => {
                        res.push(Some(s))
                    },
                    PairValueEnum::Null => {
                        res.push(None)
                    },
                    _ => {
                        return CommonError::new(&CommonDefaultErrorKind::NotMatchArgs, format!("str != {:?}", r)).to_result()
                    }
                }
            }
            Ok(res)
        } else {
            CommonError::new(&CommonDefaultErrorKind::NotMatchArgs, format!("array != {:?}", data)).to_result()
        }
    } else {
        CommonError::new(&CommonDefaultErrorKind::NotMatchArgs, format!("map != {:?}", data)).to_result()
    }
}

pub(super) use vec_if_same_len;
pub(super) use plan_select_query;
pub(super) use conn_select_query;
pub(super) use script_data_select_query;
pub(super) use get_col_ref;
pub(super) use plan_toml_select_query;