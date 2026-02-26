@echo off
setlocal ENABLEDELAYEDEXPANSION

REM =============================================
REM  start_server.bat
REM  Arranca el stack Docker. Si Docker Desktop
REM  no está activo, lo inicia y espera a que
REM  esté listo. Usa docker compose por defecto.
REM
REM  Uso:
REM    start_server.bat [perfil] [--logs]
REM
REM  perfiles:  local | rq | server   (por defecto: rq)
REM  --logs   : sigue logs tras levantar los servicios
REM =============================================

cd /d "%~dp0"

set "PROFILE=%~1"
if "%PROFILE%"=="" set "PROFILE=rq"

set "FOLLOW_LOGS=0"
if /I "%~2"=="--logs" set "FOLLOW_LOGS=1"

REM Selección de archivo compose según perfil
set "COMPOSE_FILE="
if /I "%PROFILE%"=="local" set "COMPOSE_FILE=docker-compose.local.yml"
if /I "%PROFILE%"=="rq" set "COMPOSE_FILE=docker-compose.rq.yml"
if /I "%PROFILE%"=="server" set "COMPOSE_FILE=docker-compose.server.yml"

if "%COMPOSE_FILE%"=="" (
  echo [ERROR] Perfil no reconocido: %PROFILE%
  echo         Usa: local ^| rq ^| server
  exit /b 1
)

if not exist "%COMPOSE_FILE%" (
  echo [ERROR] No se encontró "%COMPOSE_FILE%" en %cd%
  exit /b 1
)

REM Verificar docker CLI
docker --version >NUL 2>&1
if errorlevel 1 (
  echo [ERROR] No se encuentra 'docker' en PATH. Instala Docker Desktop.
  exit /b 1
)

REM Comprobar si Docker está listo (daemon activo)
docker info >NUL 2>&1
if errorlevel 1 (
  echo [INFO] Docker no está activo. Intentando iniciar Docker Desktop...
  set "DOCKER_EXE=%ProgramFiles%\Docker\Docker\Docker Desktop.exe"
  if not exist "%DOCKER_EXE%" set "DOCKER_EXE=%ProgramFiles(x86)%\Docker\Docker\Docker Desktop.exe"
  if exist "%DOCKER_EXE%" (
    start "" "%DOCKER_EXE%"
  ) else (
    echo [WARN] No se encontró Docker Desktop en la ruta estándar. Inícialo manualmente.
  )

  echo [INFO] Esperando a que Docker inicie (hasta 120s)...
  set /A __WAIT=0
  :wait_docker
  docker info >NUL 2>&1
  if not errorlevel 1 goto docker_ready
  set /A __WAIT+=1
  if %__WAIT% GEQ 120 (
    echo [ERROR] Docker no inició a tiempo. Aborta.
    exit /b 1
  )
  timeout /t 1 >NUL
  goto wait_docker
)

:docker_ready
echo [OK] Docker disponible.

REM Detectar comando compose (docker compose vs docker-compose)
set "DC=docker compose"
docker compose version >NUL 2>&1
if errorlevel 1 (
  docker-compose version >NUL 2>&1
  if errorlevel 1 (
    echo [ERROR] No se encontró ni 'docker compose' ni 'docker-compose'.
    exit /b 1
  ) else (
    set "DC=docker-compose"
  )
)

echo [INFO] Levantando stack con %COMPOSE_FILE% ...
%DC% -f "%COMPOSE_FILE%" up -d
if errorlevel 1 (
  echo [ERROR] Falló el arranque del stack. Revisa la salida de compose.
  exit /b 1
)

echo [OK] Stack levantado.

REM Abrir navegador si perfil server (puerto 30000)
if /I "%PROFILE%"=="server" (
  echo [INFO] Abriendo http://localhost:30000 ...
  start "" http://localhost:30000
)

REM Seguir logs si se solicitó
if "%FOLLOW_LOGS%"=="1" (
  echo [INFO] Siguiendo logs (Ctrl+C para salir)...
  %DC% -f "%COMPOSE_FILE%" logs -f
)

endlocal
exit /b 0
