/// Async non-blocking logger
pub fn init_logger() {
    let (non_blocking, _guard) = tracing_appender::non_blocking(std::io::stdout());

    tracing_subscriber::fmt().with_writer(non_blocking).init();

    // Keep _guard alive
    std::mem::forget(_guard);
}
