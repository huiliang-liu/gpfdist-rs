use std::collections::HashMap;

/// Parse query parameters from a URL path
/// Example: "/df/parquet?path=/data&limit=100" -> {"path": "/data", "limit": "100"}
pub fn parse_query_map(raw_path: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    
    if let Some(query_start) = raw_path.find('?') {
        let query = &raw_path[query_start + 1..];
        for pair in query.split('&') {
            if let Some(eq_pos) = pair.find('=') {
                let key = &pair[..eq_pos];
                let value = &pair[eq_pos + 1..];
                map.insert(key.to_string(), value.to_string());
            }
        }
    }
    
    map
}

/// Decode percent-encoded strings
pub fn percent_decode(s: &str) -> String {
    let mut result = String::new();
    let mut chars = s.chars();
    
    while let Some(ch) = chars.next() {
        if ch == '%' {
            // Try to decode the next two characters as hex
            let hex: String = chars.by_ref().take(2).collect();
            if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                result.push(byte as char);
            } else {
                // If decoding fails, keep the original characters
                result.push('%');
                result.push_str(&hex);
            }
        } else if ch == '+' {
            result.push(' ');
        } else {
            result.push(ch);
        }
    }
    
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_query_map_empty() {
        let map = parse_query_map("/df/parquet");
        assert!(map.is_empty());
    }

    #[test]
    fn test_parse_query_map_single() {
        let map = parse_query_map("/df/parquet?path=/data");
        assert_eq!(map.get("path"), Some(&"/data".to_string()));
    }

    #[test]
    fn test_parse_query_map_multiple() {
        let map = parse_query_map("/df/parquet?path=/data&limit=100");
        assert_eq!(map.get("path"), Some(&"/data".to_string()));
        assert_eq!(map.get("limit"), Some(&"100".to_string()));
    }

    #[test]
    fn test_percent_decode_simple() {
        assert_eq!(percent_decode("hello"), "hello");
    }

    #[test]
    fn test_percent_decode_with_encoding() {
        assert_eq!(percent_decode("hello%20world"), "hello world");
        assert_eq!(percent_decode("test%2Bvalue"), "test+value");
    }

    #[test]
    fn test_percent_decode_plus() {
        assert_eq!(percent_decode("hello+world"), "hello world");
    }
}
