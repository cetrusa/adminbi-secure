#!/bin/bash

# Agregar Docker Compose al PATH
export PATH=$PATH:/usr/local/bin

# Función para detener y eliminar un contenedor si está en ejecución
stop_and_remove_container() {
    CONTAINER_NAME=$1
    if [ "$(docker ps -q -f name=$CONTAINER_NAME)" ]; then
        echo "Deteniendo y eliminando el contenedor $CONTAINER_NAME..."
        docker stop $CONTAINER_NAME
        docker rm $CONTAINER_NAME
    elif [ "$(docker ps -a -q -f name=$CONTAINER_NAME)" ]; then
        echo "Eliminando el contenedor detenido $CONTAINER_NAME..."
        docker rm $CONTAINER_NAME
    else
        echo "El contenedor $CONTAINER_NAME no existe."
    fi
}

# Detener y eliminar contenedores específicos
stop_and_remove_container datazenith_rqworker_1
stop_and_remove_container datazenith_web_1
stop_and_remove_container datazenith_redis_1

# Limpiar el sistema Docker sin confirmación
docker system prune -f

# Navegar al directorio del proyecto
cd /var/www/datazenith

# Eliminar la base de datos SQLite
rm -f mydata.db

# Activar el entorno virtual de Python
source venv/bin/activate

# Establecer un tiempo de espera más alto para Docker Compose
export COMPOSE_HTTP_TIMEOUT=200

# Levantar los servicios con docker-compose
docker-compose -f docker-compose.rq.yml up -d --build
