@echo off
setlocal ENABLEDELAYEDEXPANSION

REM Ir al directorio del repositorio (donde está este .bat)
cd /d "%~dp0"

REM Resolver comando de Python
set "PY_CMD="
where py >NUL 2>&1 && set "PY_CMD=py -3"
if "%PY_CMD%"=="" (
  where python >NUL 2>&1 && set "PY_CMD=python"
)
if "%PY_CMD%"=="" (
  echo [ERROR] No se encontró Python en PATH. Instala Python y reintenta.
  exit /b 1
)

REM Preparar mensaje de commit: fecha + comentario del usuario (argumentos)
set "USER_MSG=%*"
if "%USER_MSG%"=="" set "USER_MSG=Actualizacion menor"
set "DATESTR=%DATE% %TIME%"

REM Agregar todos los cambios
git add -A

REM Ejecutar verificador de archivos de 0 bytes (modo pre-commit: archivos stageados)
%PY_CMD% scripts\check_zero_byte_files.py
if errorlevel 1 goto :blocked

REM Revisar si hay cambios stageados
git diff --cached --quiet
set STAGED_ERR=%ERRORLEVEL%
if "%STAGED_ERR%"=="0" goto :nochanges

git commit -m "[%DATESTR%] %USER_MSG%"
if errorlevel 1 goto :commit_fail
echo [OK] Commit creado correctamente.

REM Determinar rama actual
for /f "delims=" %%B in ('git rev-parse --abbrev-ref HEAD') do set BRANCH=%%B
if "%BRANCH%"=="" (
  echo [ERROR] No se pudo determinar la rama actual.
  exit /b 1
)

REM Verificar remoto 'origin'
git remote get-url origin >NUL 2>&1
if errorlevel 1 goto :no_remote

REM Verificar si hay upstream configurado
git rev-parse --abbrev-ref --symbolic-full-name @{u} >NUL 2>&1
if errorlevel 1 (
  echo [INFO] No hay upstream configurado para %BRANCH%. Estableciendo seguimiento con origin/%BRANCH%.
  git push -u origin %BRANCH%
) else (
  git push
)
if errorlevel 1 goto :push_fail

echo [OK] Cambios enviados a remoto correctamente.
echo.
echo Sugerencia: para revisar todo el repo por 0-bytes usa:
echo    %PY_CMD% scripts\check_zero_byte_files.py --all

endlocal
exit /b 0

:blocked
echo(
echo [BLOQUEADO] Corrige los archivos de 0 bytes no permitidos antes de hacer commit.
exit /b 1

:nochanges
echo [INFO] No hay cambios para commitear. Intentare hacer push si es necesario.
goto :after_commit

:commit_fail
echo [INFO] El commit no se realizo. Revisa la salida anterior.
exit /b 1

:no_remote
echo [ERROR] No existe el remoto 'origin'. Configura el remoto antes de continuar.
echo        Ej: git remote add origin https://github.com/usuario/repo.git
exit /b 1

:push_fail
echo [ERROR] Fallo el push. Verifica tus credenciales o permisos.
exit /b 1

:after_commit
