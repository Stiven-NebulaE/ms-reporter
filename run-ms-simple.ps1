# Script para ejecutar el microservicio ms-facts-mng-week-3
# Este script inicia todos los componentes necesarios para el desarrollo

Write-Host "Iniciando microservicio ms-facts-mng-week-3..." -ForegroundColor Green

# Verificaciones previas
Write-Host "Realizando verificaciones previas..." -ForegroundColor Yellow

# Verificar que las carpetas necesarias existan
if (-not (Test-Path "api/emi-gateway/graphql/organization-mng-organization-management")) {
    Write-Host "ADVERTENCIA: Asegúrate de tener la carpeta api/emi-gateway/graphql/organization-mng-organization-management" -ForegroundColor Yellow
    Write-Host "   Esta carpeta es necesaria para el funcionamiento correcto del API Gateway" -ForegroundColor Yellow
}

if (-not (Test-Path "playground/emi-gateway/graphql/organization-mng-organization-management")) {
    Write-Host "ADVERTENCIA: Asegúrate de tener la carpeta playground/emi-gateway/graphql/organization-mng-organization-management" -ForegroundColor Yellow
    Write-Host "   Esta carpeta es necesaria para el funcionamiento correcto del API Gateway" -ForegroundColor Yellow
}

# Verificar archivos .env
Write-Host "Verificando configuración JWT..." -ForegroundColor Yellow
if (-not (Test-Path "backend/facts-mng/.env")) {
    Write-Host "ADVERTENCIA: Asegúrate de haber configurado los JWT en el archivo .env del backend" -ForegroundColor Yellow
    Write-Host "   Archivo esperado: backend/facts-mng/.env" -ForegroundColor Yellow
}

if (-not (Test-Path "playground/emi/.env")) {
    Write-Host "ADVERTENCIA: Asegúrate de haber configurado los JWT en el archivo .env del frontend" -ForegroundColor Yellow
    Write-Host "   Archivo esperado: playground/emi/.env" -ForegroundColor Yellow
}

if (-not (Test-Path "playground/emi-gateway/.env")) {
    Write-Host "ADVERTENCIA: Asegúrate de haber configurado los JWT en el archivo .env del API Gateway" -ForegroundColor Yellow
    Write-Host "   Archivo esperado: playground/emi-gateway/.env" -ForegroundColor Yellow
}

Write-Host "Verificaciones completadas" -ForegroundColor Green
Write-Host ""

# Función para manejar la terminación del script
function Cleanup {
    Write-Host ""
    Write-Host "Deteniendo todos los procesos..." -ForegroundColor Red
    Write-Host "Para detener los servicios, cierra las terminales individuales" -ForegroundColor Yellow
    exit 0
}

# Capturar señales de terminación
Register-EngineEvent PowerShell.Exiting -Action { Cleanup }

# Iniciar el backend con Node.js 22 en una nueva terminal
Write-Host "Iniciando backend (facts-mng) en nueva terminal..." -ForegroundColor Yellow
Start-Process -FilePath "powershell" -ArgumentList "-NoExit", "-Command", "cd $(Get-Location)\backend\facts-mng; nvm use 22; Write-Host 'Backend (facts-mng) iniciado con Node.js 22' -ForegroundColor Green; Write-Host 'Directorio: ' -NoNewline; Get-Location; Write-Host ''; Write-Host 'Ejecutando: npm start' -ForegroundColor Yellow; npm start" -WindowStyle Normal
Write-Host "   Backend iniciado en nueva terminal" -ForegroundColor Green

# Esperar un poco para que el backend se inicie
Start-Sleep -Seconds 5

# Iniciar el frontend con Node.js 12 en una nueva terminal
Write-Host "Iniciando frontend (emi) en nueva terminal..." -ForegroundColor Yellow
Start-Process -FilePath "powershell" -ArgumentList "-NoExit", "-Command", "cd $(Get-Location)\playground\emi; nvm use 12; Write-Host 'Frontend (emi) iniciado con Node.js 12' -ForegroundColor Green; Write-Host 'Directorio: ' -NoNewline; Get-Location; Write-Host ''; Write-Host 'Ejecutando: yarn run dev' -ForegroundColor Yellow; yarn run dev" -WindowStyle Normal
Write-Host "   Frontend iniciado en nueva terminal" -ForegroundColor Green

# Esperar un poco para que el frontend se inicie
Start-Sleep -Seconds 12

# Iniciar el API Gateway con Node.js 10 en una nueva terminal
Write-Host "Iniciando API Gateway (emi-gateway) en nueva terminal..." -ForegroundColor Yellow
Start-Process -FilePath "powershell" -ArgumentList "-NoExit", "-Command", "cd $(Get-Location)\playground\emi-gateway; nvm use 10; Write-Host 'API Gateway (emi-gateway) iniciado con Node.js 10' -ForegroundColor Green; Write-Host 'Directorio: ' -NoNewline; Get-Location; Write-Host ''; Write-Host 'Ejecutando: node server.js' -ForegroundColor Yellow; node server.js" -WindowStyle Normal
Write-Host "   API Gateway iniciado en nueva terminal" -ForegroundColor Green

Write-Host ""
Write-Host "Todos los servicios han sido iniciados exitosamente!" -ForegroundColor Green
Write-Host ""
Write-Host "Resumen de servicios ejecutándose:" -ForegroundColor Cyan
Write-Host "   Backend (facts-mng): Terminal separada con Node.js 22" -ForegroundColor White
Write-Host "   Frontend (emi): Terminal separada con Node.js 12" -ForegroundColor White
Write-Host "   API Gateway (emi-gateway): Terminal separada con Node.js 10" -ForegroundColor White
Write-Host ""
Write-Host "URLs de acceso:" -ForegroundColor Cyan
Write-Host "   - Frontend: http://localhost:3000 (o el puerto configurado)" -ForegroundColor White
Write-Host "   - API Gateway: http://localhost:4000 (o el puerto configurado)" -ForegroundColor White
Write-Host "   - Backend: http://localhost:5000 (o el puerto configurado)" -ForegroundColor White
Write-Host ""
Write-Host "Cada servicio está ejecutándose en su propia terminal PowerShell" -ForegroundColor Yellow
Write-Host "Para detener un servicio específico, cierra su terminal correspondiente" -ForegroundColor Yellow
Write-Host "Para detener todos los servicios, cierra todas las terminales o presiona Ctrl+C aquí" -ForegroundColor Yellow
Write-Host ""

# Mantener el script ejecutándose y mostrar estado
Write-Host "Script principal ejecutándose... Presiona Ctrl+C para salir" -ForegroundColor Yellow
try {
    while ($true) {
        Start-Sleep -Seconds 10
        Write-Host "Servicios ejecutándose... (Ctrl+C para salir)" -ForegroundColor Gray
    }
}
catch {
    Cleanup
}
