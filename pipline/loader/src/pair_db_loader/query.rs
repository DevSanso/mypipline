const SELECT_PLAN : &'static str = r#"
SELECT
    p.name                  AS plan_name,
    p.type_name             AS type,
    p.interval_connection,
    p.interval_second,
    pc.id                   AS chain_pk,
    pc.next_chain_id        AS chain_id,
    pc.connection           AS chain_connection,
    pc.query,
    m.mapping_type,
    m.ranking,
    pa.data                 AS arg_data,
    pa.idx                  AS arg_idx,
    pb.bind_id,
    pb.key                  AS bind_key,
    pb.row                  AS bind_row,
    pb.idx                  AS bind_idx,
    ps.lang                 AS script_lang,
    ps.script_str                 AS script_str
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
ORDER BY
    p.name,
    pc.next_chain_id,
    m.mapping_type,
    m.ranking,
    pa.idx,
    pb.idx;
"#;