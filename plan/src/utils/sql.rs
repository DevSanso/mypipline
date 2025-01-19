pub fn change_sql_to_num_bind_support_sql(sql: &str, param: &Vec<&'_ str>) -> String {
    let mut result = sql.to_string();
    let mut offset = 0;

    for (index, key) in param.iter().enumerate() {
        let placeholder = format!("#{{{}}}", key);
        while let Some(start) = result[offset..].find(&placeholder) {
            let real_start = offset + start;
            let end = real_start + placeholder.len();
            let replacement = format!("${}", index + 1);
            result.replace_range(real_start..end, &replacement);
            offset = real_start + replacement.len();
        }
        offset = 0;
    }

    result.replace("##", "#")
} 


pub fn change_sql_to_question_mark_bind_support_sql(sql: &str, param: &Vec<&'_ str>) -> String {
    let mut result = sql.to_string();

    for key in param.iter() {
        let placeholder = format!("#{{{}}}", key);
        while let Some(start) = result.find(&placeholder) {
            let end = start + placeholder.len();
            result.replace_range(start..end, "?");
        }
    }

    result.replace("##", "#")
}