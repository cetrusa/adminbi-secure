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

@REM Forzar rebuild sin cache para que web copie codigo actualizado
echo [INFO] Rebuild sin cache (web usa COPY, no volumen)...
docker-compose -f %COMPOSE_FILE% build --no-cache web
if errorlevel 1 (
    echo [ERROR] Fallo docker-compose build web.
    exit /b 1
)

docker-compose -f %COMPOSE_FILE% up -d
if errorlevel 1 (
    echo [ERROR] Fallo docker-compose up.
    exit /b 1
)

@REM Esperar a que Redis este listo y limpiar cache de Django
echo [INFO] Esperando Redis...
ping -n 3 127.0.0.1 >nul
echo [INFO] Limpiando cache de Redis (KPIs cacheados)...
docker-compose -f %COMPOSE_FILE% exec -T redis redis-cli FLUSHALL >nul 2>&1
if errorlevel 1 (
    echo [WARN] No se pudo limpiar cache de Redis. Expirara en 15 min.
) else (
    echo [OK] Cache de Redis limpiado.
)

echo [OK] Contenedores levantados con codigo actualizado.
exit /b 0
