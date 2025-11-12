pub(super) fn convert_string_use_prefix_hard<S: AsRef<str>>(origin : &'_ str, prefix : &'_ str, array : &[S]) -> String {
    let mut ptr : Option<String> = None;

    for idx in 0..array.len() {
        let prefix_buffer = format!("{}{:03}", prefix, idx);
        ptr = Some(origin.replace(&prefix_buffer, &array[idx].as_ref().to_string()));
    }

    ptr.unwrap()
}

pub(super) fn convert_string_use_prefix_soft_question(input: &str, prefix : &'_ str, cap : usize) -> (String, Vec<(usize, usize)>) {
    let mut out = String::new();
    let mut binds = Vec::with_capacity(cap);
    let mut i = 0;
    let chars: Vec<char> = input.chars().collect();
    let mut param_index = 1;

    while i < chars.len() {
        if starts_with(&chars, i, prefix) {
            i += 7;
            let (id_str, next_idx) = read_until(&chars, i, ':');
            i = next_idx + 1;
            let (val_str, next_idx) = read_until_any(&chars, i, &[' ', '\t', '\n']);
            i = next_idx;

            if let (Some(id), Some(val)) = (parse_idx_usize_or_inf(&id_str), parse_row_usize_or_inf(&val_str)) {
                binds.push((id, val));
                out.push('?');
                param_index += 1;
            }
            continue;
        }
        out.push(chars[i]);
        i += 1;
    }

    (out, binds)
}

pub(super) fn convert_string_use_prefix_soft_num(input: &str, prefix : &'_ str, cap : usize) -> (String, Vec<(usize, usize)>) {
    let mut out = String::new();
    let mut binds = Vec::with_capacity(cap);
    let mut i = 0;
    let chars: Vec<char> = input.chars().collect();
    let mut param_index = 1;

    while i < chars.len() {
        if starts_with(&chars, i, prefix) {
            i += 7;
            let (id_str, next_idx) = read_until(&chars, i, ':');
            i = next_idx + 1;
            let (val_str, next_idx) = read_until_any(&chars, i, &[' ', '\t', '\n']);
            i = next_idx;

            if let (Some(id), Some(val)) = (parse_idx_usize_or_inf(&id_str), parse_row_usize_or_inf(&val_str)) {
                binds.push((id, val));
                out.push('$');
                out.push_str(&param_index.to_string());
                param_index += 1;
            }
            continue;
        }
        out.push(chars[i]);
        i += 1;
    }

    (out, binds)
}

fn starts_with(chars: &[char], start: usize, pat: &str) -> bool {
    let pat_chars: Vec<char> = pat.chars().collect();
    if start + pat_chars.len() > chars.len() {
        return false;
    }
    for j in 0..pat_chars.len() {
        if chars[start + j] != pat_chars[j] {
            return false;
        }
    }
    true
}

fn read_until(chars: &[char], start: usize, delim: char) -> (String, usize) {
    let mut s = String::new();
    let mut i = start;
    while i < chars.len() {
        if chars[i] == delim {
            break;
        }
        s.push(chars[i]);
        i += 1;
    }
    (s, i)
}

fn read_until_any(chars: &[char], start: usize, delims: &[char]) -> (String, usize) {
    let mut s = String::new();
    let mut i = start;
    while i < chars.len() {
        if delims.contains(&chars[i]) {
            break;
        }
        s.push(chars[i]);
        i += 1;
    }
    (s, i)
}

fn parse_idx_usize_or_inf(s: &str) -> Option<usize> {
    if s == "***" {
        Some(usize::MAX)
    } else {
        s.parse::<usize>().ok()
    }
}

fn parse_row_usize_or_inf(s: &str) -> Option<usize> {
    s.parse::<usize>().ok()
}