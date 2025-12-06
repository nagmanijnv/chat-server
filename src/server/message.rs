use chrono::Local;

pub fn timestamp() -> String {
    Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

/// format message with timestamp
pub fn format_message(msg: &str, client_id: usize) -> String {
    format!("[{}] Client {}: {}", timestamp(), client_id, msg)
}
