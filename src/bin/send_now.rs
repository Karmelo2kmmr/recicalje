use dotenv::dotenv;
use reqwest::Client;
use trend_hammer::stats_reporter::StatsReporter;
use trend_hammer::telegram::TelegramBot;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send>> {
    dotenv().ok();
    println!("🚀 Enviando reportes manuales a Telegram...");

    let client = Client::builder()
        .no_proxy()
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(20))
        .build()
        .map_err(|e| -> Box<dyn std::error::Error + Send> { Box::new(e) })?;
    let bot = TelegramBot::new().expect("Telegram Bot no configurado en .env");
    let reporter = StatsReporter::new();

    // 1. Reporte de Periodo (6h)
    println!("📊 Generando reporte de periodo...");
    match reporter.generate_period_report(&client).await {
        Ok((stats, audit)) => {
            let report = bot.format_period_report(&stats, &audit);
            bot.send_message(&report).await;
            println!("✅ Reporte de periodo enviado.");
        }
        Err(e) => println!("❌ Error en reporte de periodo: {}", e),
    }

    // 2. Reporte Diario
    println!("📈 Generando reporte diario...");
    match reporter.generate_daily_report(&client).await {
        Ok((stats, audit)) => {
            let report = bot.format_daily_report(&stats, &audit);
            bot.send_message(&report).await;
            println!("✅ Reporte diario enviado.");
        }
        Err(e) => println!("❌ Error en reporte diario: {}", e),
    }

    println!("✨ Proceso completado.");
    Ok(())
}
