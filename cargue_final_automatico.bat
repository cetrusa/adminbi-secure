@echo off
setlocal enabledelayedexpansion
chcp 65001 >nul
REM Establecer UTF-8 para Python
set PYTHONIOENCODING=utf-8

echo.
echo ============================================================
echo   DEBUG MODE - Verificando configuracion inicial
echo ============================================================
echo.

REM ============================================================
REM CONFIGURACION DE LOGGING
REM ============================================================
set "LOG_DIR=D:\Logs\DataZenithBI"
set "LOG_FILE=%LOG_DIR%\cargue_distrijass.log"
set "LOG_SUMMARY=%LOG_DIR%\cargue_summary_latest.log"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

echo. >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"
echo INICIO: %date% %time% >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"

echo.
echo ============================================================
echo   CARGUE AUTOMATICO - INFO PROVEEDORES DISTRIJASS
echo ============================================================
echo.

echo [%date% %time%] Iniciando proceso automatico completo...

REM Configuracion de rutas (MODO LOCAL - TEMPORAL)
REM set "SERVIDOR_UNC=Distrijass-bi"
REM set "UNIDAD_COMPARTIDA=d"
REM set "RUTA_UNC=\\%SERVIDOR_UNC%\%UNIDAD_COMPARTIDA%"
REM set "RUTA_BASE=%RUTA_UNC%\Distrijass\Sistema Info"
set "RUTA_BASE=D:\Distrijass\Sistema Info"
set "RUTA_DESTINO=D:\Python\DataZenithBi\Info proveedores 2025\Info proveedores.xlsx"

echo Configuracion:
echo   Modo: LOCAL (Unidad D:)
echo   Archivo destino: %RUTA_DESTINO%
echo.

echo [%date% %time%] Configuracion: >> "%LOG_FILE%"
echo [%date% %time%]   Modo: LOCAL (Unidad D:) >> "%LOG_FILE%"
echo [%date% %time%]   Archivo destino: %RUTA_DESTINO% >> "%LOG_FILE%"

REM ============================================================
REM FASE 1: COPIAR ARCHIVO
REM ============================================================

echo [%date% %time%] === FASE 1: COPIA DE ARCHIVO ===
echo [%date% %time%] === FASE 1: COPIA DE ARCHIVO === >> "%LOG_FILE%"
echo.

echo [%date% %time%] Verificando conectividad...
echo [%date% %time%] Verificando disponibilidad de unidad D:... >> "%LOG_FILE%"

REM Verificar que la unidad D: existe
if not exist "D:\" (
    echo ❌ ERROR: La unidad D:\ no esta disponible
    echo [%date% %time%] ❌ ERROR: La unidad D:\ no esta disponible >> "%LOG_FILE%"
    call :log_summary FALLO "Unidad D: no disponible"
    echo.
    echo Presione cualquier tecla para cerrar...
    pause >nul
    exit /b 1
)

echo ✅ Unidad D:\ verificada
echo [%date% %time%] ✅ Unidad D:\ verificada >> "%LOG_FILE%"

echo [%date% %time%] Buscando archivo...

echo [%date% %time%] Buscando archivo... >> "%LOG_FILE%"

REM MODO LOCAL: Buscar archivo en D:\ (SIEMPRE copiar el más reciente)
set "ARCHIVO_ENCONTRADO="

REM Buscar en D:\ con acento
set "RUTA_INFO=%RUTA_BASE%\Información\Impactos\info proveedores.xlsx"
if exist "!RUTA_INFO!" (
    set "ARCHIVO_ENCONTRADO=!RUTA_INFO!"
    echo ✅ Archivo encontrado en D:\: !ARCHIVO_ENCONTRADO!
    echo [%date% %time%] ✅ Archivo encontrado en D:\: !ARCHIVO_ENCONTRADO! >> "%LOG_FILE%"
    goto :archivo_listo
)

REM Buscar en D:\ sin acento
set "RUTA_INFO=%RUTA_BASE%\Informacion\Impactos\info proveedores.xlsx"
if exist "!RUTA_INFO!" (
    set "ARCHIVO_ENCONTRADO=!RUTA_INFO!"
    echo ✅ Archivo encontrado en D:\: !ARCHIVO_ENCONTRADO!
    echo [%date% %time%] ✅ Archivo encontrado en D:\: !ARCHIVO_ENCONTRADO! >> "%LOG_FILE%"
    goto :archivo_listo
)

REM Si no existe en D:\, verificar si al menos existe el archivo local (como último recurso)
if exist "%RUTA_DESTINO%" (
    echo ⚠️  ADVERTENCIA: No se encontró archivo en D:\, usando archivo local existente
    echo [%date% %time%] ⚠️  ADVERTENCIA: No se encontró archivo en D:\, usando archivo local existente >> "%LOG_FILE%"
    set "ARCHIVO_ENCONTRADO=%RUTA_DESTINO%"
    set "USAR_ARCHIVO_LOCAL=1"
    goto :archivo_listo
)

:archivo_listo
if "!ARCHIVO_ENCONTRADO!"=="" (
    echo ❌ ERROR: No se encontro archivo ni en D:\ ni en local
    echo [%date% %time%] ❌ ERROR CRITICO: No se encontro archivo disponible >> "%LOG_FILE%"
    call :log_summary FALLO "Archivo no disponible"
    echo.
    echo Presione cualquier tecla para cerrar...
    pause >nul
    exit /b 1
)

echo [%date% %time%] Creando directorio destino...
echo [%date% %time%] Creando directorio destino... >> "%LOG_FILE%"

if not exist "D:\Python\DataZenithBi\Info proveedores 2025" (
    mkdir "D:\Python\DataZenithBi\Info proveedores 2025"
)

REM Solo copiar si NO estamos usando el archivo local (es decir, si encontramos uno en D:\)
if not defined USAR_ARCHIVO_LOCAL set "USAR_ARCHIVO_LOCAL=0"

if "!USAR_ARCHIVO_LOCAL!"=="0" (
    echo [%date% %time%] Copiando archivo desde D:\...
    echo [%date% %time%] Copiando archivo desde D:\ >> "%LOG_FILE%"
    echo [%date% %time%] Origen: !ARCHIVO_ENCONTRADO! >> "%LOG_FILE%"
    echo [%date% %time%] Destino: %RUTA_DESTINO% >> "%LOG_FILE%"
    
    copy /Y "!ARCHIVO_ENCONTRADO!" "%RUTA_DESTINO%"

    if !ERRORLEVEL! neq 0 (
        echo ❌ ERROR: Fallo la copia desde D:\
        echo [%date% %time%] ❌ ERROR: Fallo la copia desde D:\ >> "%LOG_FILE%"
        echo [%date% %time%] ❌ Codigo de error: !ERRORLEVEL! >> "%LOG_FILE%"
        call :log_summary FALLO "Copia de archivo"
        echo.
        echo Presione cualquier tecla para cerrar...
        pause >nul
        exit /b 1
    ) else (
        echo ✅ Archivo copiado exitosamente desde D:\
        echo [%date% %time%] ✅ Archivo copiado exitosamente desde D:\ >> "%LOG_FILE%"
        
        REM Verificar fecha de modificación del archivo copiado
        for %%F in ("%RUTA_DESTINO%") do (
            echo [%date% %time%] Fecha modificación archivo destino: %%~tF >> "%LOG_FILE%"
            echo    Fecha modificación: %%~tF
        )
    )
) else (
    echo ⚠️  ADVERTENCIA: Usando archivo local existente (no se encontró en D:\)
    echo [%date% %time%] ⚠️  ADVERTENCIA: Usando archivo local existente >> "%LOG_FILE%"
    
    REM Mostrar fecha del archivo local
    for %%F in ("%RUTA_DESTINO%") do (
        echo [%date% %time%] Fecha modificación archivo local: %%~tF >> "%LOG_FILE%"
        echo    Fecha modificación: %%~tF
    )
)

echo.

REM ============================================================
REM FASE 2: VALIDAR ARCHIVO EXCEL
REM ============================================================

echo [%date% %time%] === FASE 2: VALIDACION DE ARCHIVO ===
echo [%date% %time%] === FASE 2: VALIDACION DE ARCHIVO === >> "%LOG_FILE%"
echo.

echo [%date% %time%] Validando archivo Excel...
echo [%date% %time%] Validando archivo Excel... >> "%LOG_FILE%"

REM Verificar que el archivo no este vacio (>0 bytes)
for %%A in ("%RUTA_DESTINO%") do (
    set "FILE_SIZE=%%~zA"
)

if !FILE_SIZE! equ 0 (
    echo ❌ ERROR: Archivo Excel esta vacio ^(0 bytes^)
    echo [%date% %time%] ❌ ERROR: Archivo Excel esta vacio ^(0 bytes^) >> "%LOG_FILE%"
    call :log_summary FALLO "Archivo vacio"
    echo.
    echo Presione cualquier tecla para cerrar...
    pause >nul
    exit /b 1
)

echo [%date% %time%] ✅ Archivo valido - Tamano: !FILE_SIZE! bytes
echo [%date% %time%] ✅ Archivo valido - Tamano: !FILE_SIZE! bytes >> "%LOG_FILE%"
echo.

REM ============================================================
REM FASE 3: EJECUTAR PYTHON (CON REINTENTOS)
REM ============================================================

echo [%date% %time%] === FASE 3: CARGUE PYTHON ===
echo [%date% %time%] === FASE 3: CARGUE PYTHON === >> "%LOG_FILE%"
echo.

cd /d "D:\Python\DataZenithBi\adminbi"

if not exist "cargue_infoventas_main.py" (
    echo ❌ ERROR: Script Python no encontrado
    echo [%date% %time%] ❌ ERROR: Script Python no encontrado en cargue_infoventas_main.py >> "%LOG_FILE%"
    call :log_summary FALLO "Script Python no encontrado"
    echo.
    echo Presione cualquier tecla para cerrar...
    pause >nul
    exit /b 1
)

REM Configurar reintentos
set "MAX_REINTENTOS=3"
set "INTENTO=1"
set "PYTHON_RESULT=1"

:reintentar_cargue
if !INTENTO! leq !MAX_REINTENTOS! (
    echo [%date% %time%] Intento !INTENTO! de !MAX_REINTENTOS!...
    echo [%date% %time%] Intento !INTENTO! de !MAX_REINTENTOS!... >> "%LOG_FILE%"

    echo [%date% %time%] Activando entorno virtual...
    echo [%date% %time%] Activando entorno virtual... >> "%LOG_FILE%"
    
    if exist ".venv\Scripts\activate.bat" (
        call ".venv\Scripts\activate.bat" >nul
    )

    echo [%date% %time%] Ejecutando cargue Python...
    echo [%date% %time%] Ejecutando cargue Python... >> "%LOG_FILE%"
    echo.

    powershell -Command "& '.venv\Scripts\python.exe' cargue_infoventas_main.py --base distrijass --archivo '%RUTA_DESTINO%'" >> "%LOG_FILE%" 2>&1

    set "PYTHON_RESULT=!ERRORLEVEL!"

    if !PYTHON_RESULT! equ 0 (
        echo [%date% %time%] ✅ Intento !INTENTO! exitoso
        echo [%date% %time%] ✅ Intento !INTENTO! exitoso >> "%LOG_FILE%"
        goto :cargue_exitoso
    ) else (
        if !INTENTO! lss !MAX_REINTENTOS! (
            echo [%date% %time%] ❌ Intento !INTENTO! fallo - Esperando 30 segundos...
            echo [%date% %time%] ❌ Intento !INTENTO! fallo - Esperando 30 segundos... >> "%LOG_FILE%"
            timeout /t 30 /nobreak
            
            set /a INTENTO=!INTENTO! + 1
            goto :reintentar_cargue
        ) else (
            goto :cargue_fallo
        )
    )
)

REM ============================================================
REM ETIQUETAS DE RESULTADO
REM ============================================================

:cargue_exitoso
echo.
echo [%date% %time%] === PROCESO FINALIZADO ===
echo [%date% %time%] === PROCESO FINALIZADO === >> "%LOG_FILE%"
echo [%date% %time%] ✅ CARGUE COMPLETADO EXITOSAMENTE
echo [%date% %time%] ✅ CARGUE COMPLETADO EXITOSAMENTE >> "%LOG_FILE%"
echo.
echo ============================================================
echo   RESUMEN FINAL
echo ============================================================
echo [%date% %time%] === RESUMEN FINAL === >> "%LOG_FILE%"

if "!USAR_ARCHIVO_LOCAL!"=="1" (
    echo ✅ Archivo: %RUTA_DESTINO% (ARCHIVO LOCAL)
    echo ⚠️  AVISO: Se uso archivo local (unidad de red no disponible)
    echo [%date% %time%] ✅ Archivo: %RUTA_DESTINO% (ARCHIVO LOCAL) >> "%LOG_FILE%"
    echo [%date% %time%] ⚠️  AVISO: Se uso archivo local (unidad de red no disponible) >> "%LOG_FILE%"
) else (
    echo ✅ Archivo: !ARCHIVO_ENCONTRADO!
    echo [%date% %time%] ✅ Archivo: !ARCHIVO_ENCONTRADO! >> "%LOG_FILE%"
)

echo ✅ Destino: %RUTA_DESTINO%
echo ✅ Base de datos: distrijass - CARGUE EXITOSO
echo ============================================================
echo [%date% %time%] ✅ Destino: %RUTA_DESTINO% >> "%LOG_FILE%"
echo [%date% %time%] ✅ Base de datos: distrijass - CARGUE EXITOSO >> "%LOG_FILE%"
echo [%date% %time%] FIN: %date% %time% >> "%LOG_FILE%"

REM ============================================================
REM ENVIO DE REPORTE POR EMAIL (OPCIONAL)
REM ============================================================
echo.
echo [%date% %time%] === FASE 4: ENVIO DE REPORTE ===
echo Intentando enviar reporte por correo...

REM Descomenta las siguientes lineas para habilitar envio de reportes:
REM echo [%date% %time%] Ejecutando send_cargue_report.py... >> "%LOG_FILE%"
REM cd /d "D:\Python\DataZenithBi\adminbi"
REM call .venv\Scripts\activate.bat
REM python send_cargue_report.py --log "%LOG_FILE%" --email "admin@distrijass.com" >> "%LOG_FILE%" 2>&1
REM if errorlevel 1 (
REM     echo [%date% %time%] ⚠️  No se pudo enviar reporte (no es fatal) >> "%LOG_FILE%"
REM ) else (
REM     echo [%date% %time%] ✅ Reporte enviado exitosamente >> "%LOG_FILE%"
REM )

call :log_summary EXITOSO "Cargue completado correctamente"

echo.
echo Log guardado en: %LOG_FILE%
echo.
exit /b 0

:cargue_fallo
echo.
echo [%date% %time%] === PROCESO FINALIZADO CON ERROR ===
echo [%date% %time%] === PROCESO FINALIZADO CON ERROR === >> "%LOG_FILE%"
echo [%date% %time%] ❌ ERROR EN EL CARGUE DESPUES DE !MAX_REINTENTOS! INTENTOS
echo [%date% %time%] ❌ ERROR EN EL CARGUE DESPUES DE !MAX_REINTENTOS! INTENTOS >> "%LOG_FILE%"
echo [%date% %time%] Codigo de error: !PYTHON_RESULT!
echo [%date% %time%] Codigo de error: !PYTHON_RESULT! >> "%LOG_FILE%"

call :log_summary FALLO "Python cargue - Codigo !PYTHON_RESULT!"

echo.
echo Log guardado en: %LOG_FILE%
echo.
echo Presione cualquier tecla para cerrar...
pause >nul
exit /b !PYTHON_RESULT!

REM ============================================================
REM FUNCION: Registrar resumen en archivo de resumen rapido
REM ============================================================
:log_summary
setlocal enabledelayedexpansion
set "STATUS=%~1"
set "DETAIL=%~2"

if not exist "%LOG_SUMMARY%" (
    echo. > "%LOG_SUMMARY%"
    echo ============================================================ >> "%LOG_SUMMARY%"
    echo RESUMEN DE CARGUES - DISTRIJASS >> "%LOG_SUMMARY%"
    echo ============================================================ >> "%LOG_SUMMARY%"
    echo. >> "%LOG_SUMMARY%"
)

echo [%date% %time%] Estado: %STATUS% - %DETAIL% >> "%LOG_SUMMARY%"

endlocal
exit /b 0