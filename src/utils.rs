pub fn assert_send<F: Send>() {}
pub fn assert_sync<F: Sync>() {}
pub fn assert_send_sync<F: Sync + Send>() {}
