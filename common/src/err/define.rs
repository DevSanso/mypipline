use crate::err::impl_err_mod;

impl_err_mod!(collection, [
    (GenResultIsNoneError, "gen function is return none", "if pool gen function errors, data is none check process"),
    (MaxSizedError, "memeory or pool is used Max size", "can't alloc new memory"),
    (PoolNotSetError, "pool state is not init", "check code, create pool function"),
    (PoolGetItemError, "failed pool item", "mayby pool ls used max or gen function failed, check env")
]);

impl_err_mod!(connection, [
    (GetConnectionFailedError, "get other process connection", "check other process state"),
    (NotMatchArgsLenError, "query bound args count not mathcing", "query parameter length not maching, check bound varibles"),
    (ResponseScanError, "connection response data read error", "check server state or error handling code"),
    (CommandRunError, "running command or query is error", "check query or command"),
    (ConnectionApiCallError, "connection api function return error", "check server env or process state")
]);

impl_err_mod!(system, [
    (OverflowSizeError, "overflow size error", "check array size or range size")
]);

impl_err_mod!(no_category, [
    (UnknownError, "unknown error","check system")
]);

