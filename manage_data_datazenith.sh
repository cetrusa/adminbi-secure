#!/bin/bash

# Agregar Docker Compose al PATH
export PATH=$PATH:/usr/local/bin

# Limpiar el sistema Docker sin confirmación
docker system prune -a --volumes -f

# Navegar al directorio del proyecto
cd /var/www/datazenith || exit

# Verificar y eliminar la base de datos SQLite si existe
if [ -f mydata.db ]; then
    rm -f mydata.db
    echo "Base de datos SQLite eliminada."
else
    echo "No se encontró la base de datos SQLite."
fi

# Verificar y eliminar archivos de la carpeta media si existen
if [ "$(ls -A /var/www/datazenith/media/)" ]; then
    rm -f /var/www/datazenith/media/*.*
    echo "Archivos en la carpeta media eliminados."
else
    echo "No se encontraron archivos en la carpeta media."
fi
