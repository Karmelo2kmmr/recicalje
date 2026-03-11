use reqwest::Client;
use serde_json::Value;
use chrono::{Utc, Timelike};

#[tokio::main]
async fn main() {
    let client = Client::new();
    
    // Get current time and round down to nearest 5 mins
    let now = Utc::now();
    let current_mins = now.minute();
    let bucket_mins = (current_mins / 5) * 5;
    let bucket_time = now.with_minute(bucket_mins).unwrap().with_second(0).unwrap().with_nanosecond(0).unwrap();
    let ts = bucket_time.timestamp();
    
    // Construct slugs for current and next 2 windows
    for i in 0..3 {
        let loop_ts = ts + (i * 300);
        let slug = format!("btc-updown-5m-{}", loop_ts);
        let url = format!("https://gamma-api.polymarket.com/markets/slug/{}", slug);
        
        println!("Checking slug: {}", slug);
        match client.get(&url).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    let m: Value = resp.json().await.unwrap();
                    println!("  -> FOUND: {} | ID: {}", m["question"], m["id"]);
                } else {
                    println!("  -> Not found (Status: {})", resp.status());
                }
            }
            Err(e) => println!("  -> Error: {}", e),
        }
    }
}
