use percent_encoding::percent_decode_str;

pub(crate) fn urldecode_some(s: Option<&str>) -> String {
    if let Some(s) = s {
        let res = &percent_decode_str(s).decode_utf8();
        match res {
            Err(_) => String::new(),
            Ok(v) => v.to_string(),
        }
    }
    else {
        String::new()
    }
}

pub(crate) fn until_err<T, E>(err: &mut &mut Result<(), E>, item: Result<T, E>) -> Option<T> {
    match item {
        Ok(item) => Some(item),
        Err(e) => {
            **err = Err(e);
            None
        }
    }
}
