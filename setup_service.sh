#!/bin/bash
# setup_service.sh - Configuración automática para Google Cloud (Linux)

set -e

REPO_DIR="$HOME/recicalje"
SERVICE_NAME="bot-reciclaje"
DAEMON_SERVICE_NAME="clob-daemon"

echo "🚀 Configurando servicios de Systemd para Arbitrage Hammer..."

# 1. Crear el servicio para el Daemon de Polymarket
cat <<EOF | sudo tee /etc/systemd/system/${DAEMON_SERVICE_NAME}.service
[Unit]
Description=Polymarket CLOB Daemon
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=${REPO_DIR}
ExecStart=/usr/bin/python3 clob_daemon.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

# 2. Crear el servicio para el Bot principal
cat <<EOF | sudo tee /etc/systemd/system/${SERVICE_NAME}.service
[Unit]
Description=Arbitrage Hammer Bot
After=network.target ${DAEMON_SERVICE_NAME}.service
Requires=${DAEMON_SERVICE_NAME}.service

[Service]
Type=simple
User=$USER
WorkingDirectory=${REPO_DIR}
Environment=RUST_LOG=info
ExecStart=${REPO_DIR}/target/release/arbitrage_hammer
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# 3. Recargar y habilitar
sudo systemctl daemon-reload
sudo systemctl enable ${DAEMON_SERVICE_NAME}
sudo systemctl enable ${SERVICE_NAME}

echo "✅ Servicios instalados y habilitados."
echo "Usa 'sudo systemctl start ${SERVICE_NAME}' para iniciar todo."
