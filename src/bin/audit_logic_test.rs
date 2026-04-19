use std::time::Instant;

// Minimal reproduction of the EntryType and pricing logic
#[derive(Debug, Clone, PartialEq)]
enum EntryType {
    Dip,
    TriggerDirect,
    FullRecovery,
    DeepNets,
}

fn calculate_dynamic_sl(entry_price: f64, entry_type: &EntryType) -> f64 {
    // Red Profunda: NO Stop Loss
    if *entry_type == EntryType::DeepNets {
        return 0.0;
    }

    // Full Recovery: Relative SL (50% below entry)
    if entry_price <= 0.10 || *entry_type == EntryType::FullRecovery {
        return entry_price * 0.5;
    }

    0.655
}

fn calculate_pnl(entry: f64, exit: f64, _side: &str) -> f64 {
    // Simplified direction-aware PnL
    ((exit - entry) / entry) * 100.0
}

fn parse_target_price(question: &str) -> Option<f64> {
    use regex::Regex;
    let re = Regex::new(r"\$(\d{1,3}(,\d{3})*(\.\d+)?)").ok()?;
    if let Some(caps) = re.captures(question) {
        if let Some(matched) = caps.get(1) {
            let price_str = matched.as_str().replace(',', "");
            return price_str.parse().ok();
        }
    }
    None
}

fn main() {
    println!("--- AUDIT LOGIC TEST (STRATEGY REFINEMENT) ---");

    // 1. Test Full Recovery Scaling
    let fr_entry_1 = 0.06;
    let fr_entry_2 = 0.215; // New scaling price
    let fr_sl = calculate_dynamic_sl(fr_entry_1, &EntryType::FullRecovery);

    // Average entry for $5 @ 0.06 and $4 @ 0.215
    let total_cost = (5.0 * fr_entry_1) + (4.0 * fr_entry_2);
    let avg_entry = total_cost / 9.0;

    println!("Full Recovery Entry 1: {}", fr_entry_1);
    println!("Full Recovery SL (Relative 50%): {}", fr_sl);
    println!("Full Recovery Entry 2 (Scaling): {}", fr_entry_2);
    println!("Full Recovery Avg Entry: {:.4}", avg_entry);

    assert!(fr_sl < fr_entry_1, "SL must be below entry");
    assert_eq!(fr_sl, 0.03, "SL should be 0.03 (50% of 0.06)");

    // 2. Test Red Profunda (Deep Nets)
    let dn_entry = 0.008; // < 0.01
    let dn_sl = calculate_dynamic_sl(dn_entry, &EntryType::DeepNets);
    let dn_tp = 0.05;

    println!("\nDeep Nets Entry: {}", dn_entry);
    println!("Deep Nets SL: {} (Should be 0.0)", dn_sl);
    println!("Deep Nets TP: {}", dn_tp);

    assert_eq!(dn_sl, 0.0, "Deep Nets should have no SL");

    // 3. Test Red Profunda Forced Exit (PnL)
    let dn_exit_at_close = 0.05; // Sold before expiration
    let pnl = calculate_pnl(dn_entry, dn_exit_at_close, "UP");
    println!("Deep Nets PnL if sold at 0.05: {:.1}%", pnl);

    assert!(pnl > 0.0, "Should be profitable");
    assert_eq!(pnl, 525.0, "PnL check");

    // 4. Test Target Price Parsing
    let q1 = "Will Bitcoin be above $70,631.65 at 8 PM ET?";
    let t1 = parse_target_price(q1).expect("Should parse target price");
    println!("\nParsed Target Price 1: {}", t1);
    assert_eq!(t1, 70631.65);

    let q2 = "Will Ethereum be above $4,123.45 at 9 PM ET?";
    let t2 = parse_target_price(q2).expect("Should parse target price");
    println!("Parsed Target Price 2: {}", t2);
    assert_eq!(t2, 4123.45);

    // 5. Test Asset Distance Filter
    let target = 70000.0;
    let threshold = 103.0;

    let current_up_ok = 70104.0;
    let current_up_fail = 70102.0;
    let current_down_ok = 69896.0;
    let current_down_fail = 69898.0;

    let dist_up_ok = current_up_ok - target;
    let dist_up_fail = current_up_fail - target;
    let dist_down_ok = target - current_down_ok;
    let dist_down_fail = target - current_down_fail;

    println!(
        "\nDistance UP (70104 - 70000): {} (OK? {})",
        dist_up_ok,
        dist_up_ok >= threshold
    );
    println!(
        "Distance UP (70102 - 70000): {} (OK? {})",
        dist_up_fail,
        dist_up_fail >= threshold
    );
    println!(
        "Distance DOWN (70000 - 69896): {} (OK? {})",
        dist_down_ok,
        dist_down_ok >= threshold
    );
    println!(
        "Distance DOWN (70000 - 69898): {} (OK? {})",
        dist_down_fail,
        dist_down_fail >= threshold
    );

    assert!(dist_up_ok >= threshold);
    assert!(dist_up_fail < threshold);
    assert!(dist_down_ok >= threshold);
    assert!(dist_down_fail < threshold);

    assert!(dist_down_fail < threshold);

    // 6. Test ETH/XRP Percentage Filter
    let eth_target = 3000.0;
    let eth_up_target = eth_target * 1.0018; // 3005.4
    let eth_down_target = eth_target * 0.9980; // 2994.0

    let current_eth_up_ok = 3006.0;
    let current_eth_up_fail = 3005.0;
    let current_eth_down_ok = 2993.0;
    let current_eth_down_fail = 2995.0;

    println!(
        "\nETH Percentage UP (3006 >= 3005.4): {}",
        current_eth_up_ok >= eth_up_target
    );
    println!(
        "ETH Percentage UP (3005 >= 3005.4): {}",
        current_eth_up_fail >= eth_up_target
    );
    println!(
        "ETH Percentage DOWN (2993 <= 2994): {}",
        current_eth_down_ok <= eth_down_target
    );
    println!(
        "ETH Percentage DOWN (2995 <= 2994): {}",
        current_eth_down_fail <= eth_down_target
    );

    assert!(current_eth_up_ok >= eth_up_target);
    assert!(!(current_eth_up_fail >= eth_up_target));
    assert!(current_eth_down_ok <= eth_down_target);
    assert!(!(current_eth_down_fail <= eth_down_target));

    println!("\n✅ ALL LOGIC TESTS PASSED!");
}
