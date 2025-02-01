use scylla::frame::response::result::CqlValue;

pub(crate) type Response1 = (
    CqlValue,
);

pub(crate) type Response2 = (
    CqlValue, CqlValue
);

pub(crate) type Response3 = (
    CqlValue, CqlValue, CqlValue
);

pub(crate) type Response4 = (
    CqlValue, CqlValue, CqlValue, CqlValue
);
