# Arbitrage Hammer — Alpha Lobo V2 🐺

Bot de arbitraje y seguimiento de tendencia cross-venue entre **Polymarket** y **Kalshi**, utilizando **Binance** como fuente de verdad (Price to Beat).

## 🚀 Estrategia: Alpha Lobo V2

El bot opera bajo una lógica de "seguimiento de tendencia confirmada". No entra por simple diferencia de precio, sino cuando Binance confirma que el precio se está moviendo con fuerza hacia un lado.

### 1. PTB Lock (Bloqueo de 3 Minutos)
Para evitar la volatilidad errática del inicio de los mercados (velas de 15 min):
- Al detectar un nuevo mercado, el bot **espera 180 segundos**.
- Tras la espera, captura el precio actual de Binance y lo fija como el **Price to Beat (PTB)**.
- Todas las distancias de entrada se miden desde este punto fijo.

### 2. Filtros de Entrada (Distancia Dinámica)
El bot solo entra si el precio ha viajado una distancia mínima desde el PTB, ajustada por volatilidad (Z-Score):
- **BTC:** Nivel 1: $60 | Nivel 2: $90 | Nivel 3: $140
- **ETH:** Nivel 1: $1.50 | Nivel 2: $2.50 | Nivel 3: $4.00
- Los umbrales suben automáticamente si el mercado está muy volátil.

### 3. Salida por Retroceso de Binance (Retrace Stop)
Una vez dentro, el bot vigila a Binance. Si el precio retrocede contra nuestra posición, cerramos para proteger capital:
- **Tier 1 (Entrada corta):** Retroceso de $24 en BTC cierra la operación.
- **Tier 2 (Entrada media):** Retroceso de $41 en BTC cierra la operación.
- **Tier 3 (Entrada explosiva):** Retroceso de $70 en BTC cierra la operación.

### 4. Hedge Cruzado (Protección de Liquidez)
Si se toca el Stop Loss en una plataforma pero **no hay liquidez** (Bid < 0.23):
- El bot abre inmediatamente una **posición opuesta de $5** en la otra plataforma.
- Esta cobertura (Hedge) tiene su propio **SL dinámico (Entry - 0.18)** y **TP fijo (0.95)**.
- El mercado original se marca como "Quemado".

### 5. Estado de Mercado Quemado (Burned)
Si una operación termina en Stop Loss o activa un Hedge, el mercado se marca como **Quemado**.
- **No se abrirán nuevas entradas** en ese activo durante el resto del ciclo de 15 minutos.
- Evita "venganza" contra el mercado o sobre-operar en activos problemáticos.

## 🛠️ Configuración (.env)

| Variable | Descripción |
|----------|-------------|
| `POSITION_SIZE` | Tamaño fijo de cada entrada ($5.0) |
| `PTB_LOCK_DELAY_SECONDS` | Tiempo de espera inicial (180s) |
| `SL_LIQUIDITY_THRESHOLD` | Umbral para disparar Hedge (0.23) |
| `ALLOW_CROSS_VENUE_HEDGE` | Activar/Desactivar cobertura automática |

## 💻 Ejecución

El bot está desarrollado en Rust para máxima velocidad.

```powershell
# Ejecutar en modo desarrollo
cargo run

# Compilar para producción (máximo rendimiento)
cargo build --release
./target/release/arbitrage_hammer.exe
```

---
**Desarrollado por Antigravity AI**
