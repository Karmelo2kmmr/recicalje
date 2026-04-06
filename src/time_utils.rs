use chrono::{DateTime, Utc};
use chrono_tz::America::New_York;

pub fn new_york_now() -> DateTime<chrono_tz::Tz> {
    Utc::now().with_timezone(&New_York)
}

pub fn to_new_york(datetime_utc: DateTime<Utc>) -> DateTime<chrono_tz::Tz> {
    datetime_utc.with_timezone(&New_York)
}
