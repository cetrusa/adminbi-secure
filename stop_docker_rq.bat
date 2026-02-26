@echo off
setlocal
set COMPOSE_FILE=docker-compose.rq.yml

docker info >nul 2>&1
if errorlevel 1 (
    echo [WARN] Docker no esta activo. Nada que apagar.
    exit /b 0
)

docker-compose -f %COMPOSE_FILE% down --remove-orphans
if errorlevel 1 (
    echo [ERROR] Fallo docker-compose down.
    exit /b 1
)

echo [OK] Contenedores detenidos.
exit /b 0
