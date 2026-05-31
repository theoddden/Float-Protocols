//! GNSS Service and RTC Synchronization
//!
//! GNSS positioning and time discipline from BG95-S5.
//! NMEA sentence parsing and Linux RTC synchronization.

pub mod gnss;
pub mod rtc;

pub use gnss::{GnssService, GnssFix};
pub use rtc::RtcSync;
