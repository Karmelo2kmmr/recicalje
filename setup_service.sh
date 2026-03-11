#!/延ash
# Script para configurar el servicio del Bot de Reciclaje en Google Cloud

echo "⚙️ Configurando servicio bot-reciclaje.service..."

sudo bash -c 'cat > /etc/systemd/system/bot-reciclaje.service <<EOF 
[Unit]
Description=Polymarket Reciclaje Bot
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$(pwd)
ExecStart=/home/$USER/.cargo/bin/cargo run --release
Restart=always
RestartSec=300
Environment="PATH=/home/$USER/.cargo/bin:/usr/bin:/bin"

[Install]
WantedBy=multi-user.target
EOF'

echo "🔄 Recargando systemd y reiniciando bot..."
sudo systemctl daemon-reload
sudo systemctl enable bot-reciclaje
sudo systemctl restart bot-reciclaje

echo "✅ ¡Listo! El bot se reiniciará automáticamente tras 5 min de caída y al arrancar el servidor."
echo "Puedes ver los logs con: sudo journalctl -u bot-reciclaje -f"
