@echo off
echo ==========================================
echo Starte IoT-Sensordaten System (Gruppe 8)
echo ==========================================

echo.
echo [1] Starte RabbitMQ via Docker Compose...
docker-compose up -d

echo.
echo [2] Warte 10 Sekunden, bis RabbitMQ vollstaendig hochgefahren ist...
timeout /t 10 /nobreak

echo.
echo [3] Richte RabbitMQ Topologie (Exchanges/Queues) ein...
python -m scripts.setup_rabbitmq

echo.
echo [4] Starte alle Consumer und Producer in neuen Fenstern...

:: Startet den Producer
start "Producer" cmd /k "python -m producer.sensor_producer"

:: Startet ZWEI Analyse-Consumer mit den jeweils geforderten Argumenten
start "Analyse Consumer (Temperatur)" cmd /k "python -m analysis.consumer temperature"
start "Analyse Consumer (Environment)" cmd /k "python -m analysis.consumer environment"

:: Startet den Alarm-Consumer
start "Alarm Consumer" cmd /k "python -m alarm.consumer"

:: Startet deinen Monitoring-Consumer
start "Monitoring Dashboard" cmd /k "python -m monitoring.monitoring_consumer"

echo.
echo Alle Komponenten wurden erfolgreich gestartet!
pause