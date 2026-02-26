@echo off
setlocal
set COMPOSE_FILE=docker-compose.rq.yml
set DOCKER_DESKTOP="C:\Program Files\Docker\Docker\Docker Desktop.exe"

where docker >nul 2>&1
if errorlevel 1 (
    echo [ERROR] No se encontro docker en PATH. Instale Docker Desktop o ajuste el PATH.
    exit /b 1
)

docker info >nul 2>&1
if errorlevel 1 (
    echo [INFO] Docker no esta activo. Iniciando Docker Desktop...
    if exist %DOCKER_DESKTOP% (
        start "" %DOCKER_DESKTOP%
    ) else (
        echo [WARN] No se encontro Docker Desktop en %DOCKER_DESKTOP%.
    )
)

set /a retries=30
:wait_docker
    docker info >nul 2>&1
    if not errorlevel 1 goto docker_ready
    set /a retries-=1
    if %retries% leq 0 (
        echo [ERROR] Docker no inicio a tiempo.
        exit /b 1
    )
    echo [INFO] Esperando que Docker inicie... (%retries% intentos restantes)
    ping -n 2 127.0.0.1 >nul
    goto wait_docker

:docker_ready
echo [INFO] Docker disponible. Levantando contenedores RQ...
docker-compose -f %COMPOSE_FILE% up --build
if errorlevel 1 (
    echo [ERROR] Fallo docker-compose up.
    exit /b 1
)

echo [OK] Contenedores levantados.
exit /b 0
