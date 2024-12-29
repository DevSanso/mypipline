use scylla::frame::response::result::CqlValue;


pub(crate) type Response = (
    CqlValue, CqlValue, CqlValue, CqlValue, CqlValue,
    CqlValue, CqlValue, CqlValue, CqlValue, CqlValue,
    CqlValue, CqlValue, CqlValue, CqlValue, CqlValue
);

