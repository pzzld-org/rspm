use super::event::{MarketEvent, parse_frame};

/// One received WebSocket data frame, stamped at receipt.
///
/// `seq` is contiguous per connection while nothing is dropped, so a jump means
/// frames were dropped on a full queue (the gap marker); a new `conn_id` means a
/// reconnect.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub conn_id: u64,
    pub seq: u64,
    pub received_at_ns: i64,
    pub bytes: Vec<u8>,
}

impl Frame {
    /// Parse the frame into events. Object and array frames are both accepted;
    /// anything unknown or unparseable comes back as [`MarketEvent::Other`].
    pub fn events(&self) -> Vec<MarketEvent> {
        parse_frame(&self.bytes)
    }
}
