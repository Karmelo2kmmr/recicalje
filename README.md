# 🔨 Arbitrage Hammer — Alpha Lobo V2 🐺
### Sistema Avanzado de Arbitraje Cross-Venue & Trend Following

---

## 📜 1. Tesis Operativa: "Alpha Lobo V2"

El **Arbitrage Hammer** no es un bot de arbitraje estadístico tradicional. Su tesis central se basa en la **Confirmación de Tendencia con Fuente de Verdad Externa (Binance)**. 

En lugar de buscar ineficiencias de milisegundos entre orderbooks (donde la latencia suele ganar), el bot identifica cuándo un mercado de predicción (Polymarket o Kalshi) está "lento" para reaccionar ante un movimiento real y confirmado en el mercado de spot/perpetuos de Binance.

### El Concepto "Price to Beat" (PTB)
Para evitar el ruido y los "fakeouts" al inicio de cada vela de 15 minutos, el bot implementa un bloqueo temporal (**PTB Lock**):
1.  **Observación:** Durante los primeros 180 segundos de un mercado, el bot solo observa.
2.  **Fijación:** Transcurrido ese tiempo, captura el precio de Binance y lo define como el **Price to Beat**.
3.  **Ejecución:** Solo se considera una entrada si el precio actual de Binance se aleja del PTB una distancia mínima predefinida, validando así que la tendencia tiene "momento" real.

---

## 🏗️ 2. Arquitectura del Sistema

El bot está construido con una arquitectura híbrida para maximizar la velocidad de cálculo y la compatibilidad con APIs complejas:

*   **Core (Rust):** El motor principal, gestor de riesgos, descubrimiento de mercados y lógica de ejecución. Rust garantiza seguridad de memoria y ejecución multihilo sin bloqueos.
*   **CLOB Daemon (Python):** Un microservicio especializado que gestiona la conexión con el CLOB V2 de Polymarket. Maneja la firma de mensajes EIP-712 y la reconciliación on-chain mediante el SDK oficial de Polymarket.
*   **Persistence Layer:** Utiliza `open_positions.json` para mantener el estado operativo. El bot es "stateless" en memoria pero "stateful" en disco, lo que permite reinicios sin perder el control de las posiciones.

---

## 🛠️ 3. Procesos Principales

### A. Market Discovery & Twin Linking
El bot escanea simultáneamente Kalshi y Polymarket buscando "twins" (mercados gemelos).
*   **Criterio de Match:** Deben compartir el mismo activo (BTC, ETH, SOL, XRP), la misma dirección (Up/Down) y la misma ventana de tiempo de resolución.
*   **Arbitraje:** Si ambos mercados están disponibles, el bot elige el que ofrezca el mejor precio (Ask más bajo) para maximizar el margen.

### B. Motor de Entrada (Entry Engine)
La entrada se valida mediante tres capas:
1.  **Distancia Dinámica:** El precio debe superar el PTB + un umbral ajustado por el **Z-Score** de volatilidad (Calculado sobre 200 periodos de 1 min).
2.  **Sizing Engine:** Calcula el tamaño de la posición basado en el capital disponible y los límites de exposición configurados en `.env`.
3.  **Trade Validator:** Verifica liquidez en el orderbook y spreads para asegurar que la entrada sea viable.

### C. Gestión de Salida (Exit Strategies)
*   **Take Profit (TP):** Cierre automático si el precio llega al nivel objetivo (ej. 0.97).
*   **Hard Stop Loss (SL):** Cierre de emergencia si el precio cae por debajo de un umbral crítico.
*   **Binance Retrace:** Si el precio de Binance retrocede un porcentaje específico de la ganancia obtenida desde la entrada, el bot cierra la posición para proteger capital ("Trailing Stop" basado en spot).

---

## 🛡️ 4. Position Recovery Manager (P0 Hardening)
*Implementación crítica añadida el 01/05/2026*

Para resolver el problema de las "posiciones a la deriva" (posiciones que quedan abiertas por fallos de red, mercados expirados o errores de llenado zero-fill), se ha implementado un gestor de recuperación independiente:

1.  **Detección de Adrifts:** En cada tick, el bot detecta posiciones que ya no aparecen en el "Market Discovery" pero siguen marcadas como abiertas.
2.  **Verificación Externa de Verdad:**
    *   **Kalshi:** Consulta el portfolio real del usuario vía API. Si la posición no existe o el balance es 0, se auto-reconcilia como cerrada.
    *   **Polymarket:** Consulta el balance on-chain del `token_id` específico. 
3.  **Escalera de Salida Agresiva (Stepped Exits):** Si un cierre falla, el bot no se rinde. Entra en estado `RecoveryPending` e intenta cerrar con precios progresivamente más agresivos (slippage del 5%, luego 15%, luego precio nuclear de 0.01) hasta confirmar el cierre.
4.  **Safe Mode:** Si hay posiciones en estado de error o recuperación, el bot activa el **Modo Seguro**, bloqueando cualquier nueva entrada hasta que el inventario esté limpio.

---

## 📈 5. Fuentes de Información

*   **Binance API:** Fuente primaria de precio y volatilidad (Tickers de 1m y 15m).
*   **Kalshi API:** Ejecución de opciones binarias reguladas.
*   **Polymarket CLOB V2:** Ejecución de mercados de predicción descentralizados (Polygon Network).
*   **Telegram Bot:** Canal de alertas en tiempo real para ejecuciones, errores y estados de cuenta.

---

## 📝 6. Historial de Modificaciones (Log de Ingeniería)

*   **v2.0.0 (Base):** Implementación de Alpha Lobo V2 con PTB Lock.
*   **v2.1.0:** Añadido soporte para SOL y XRP. Refactorización de `volatility.rs`.
*   **v2.2.0 (P0 Fix):** 
    *   Eliminación de estados ambiguos (`StopFailed`, `ExpiredResolved`).
    *   Introducción de `Position Recovery Manager`.
    *   Implementación de `Safe Mode` global.
    *   Añadido cooldown anti-spam de 60s en alertas de Telegram.
    *   Limpieza de repositorio y optimización de `.gitignore`.

---

## 🚀 Cómo Operar el Bot

1.  **Configuración:** Editar `.env` con las API Keys y parámetros de riesgo.
2.  **Lanzamiento:** Ejecutar `launch_all.ps1` o los archivos `.bat` correspondientes.
3.  **Monitoreo:** Revisar la consola y el canal de Telegram para STATS de mercado y alertas de ejecución.

---
> **Nota para IAs Futuras:** Este documento es la "fuente de verdad" del contexto operativo. Siempre leer este README antes de proponer cambios en `src/main.rs` o los motores de riesgo. 🐺🔨
