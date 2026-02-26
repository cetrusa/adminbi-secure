"""
Middleware para detectar consultas lentas y problemas de rendimiento en Django
"""
import time
import logging
from django.db import connection
from django.conf import settings
from django.utils.deprecation import MiddlewareMixin

logger = logging.getLogger(__name__)

class PerformanceMiddleware(MiddlewareMixin):
    """
    Middleware para monitorear el rendimiento de las vistas Django
    """
    
    def process_request(self, request):
        # Marcar inicio de request
        request._performance_start_time = time.time()
        request._performance_queries_start = len(connection.queries)
        return None
    
    def process_response(self, request, response):
        # Calcular tiempo total
        if hasattr(request, '_performance_start_time'):
            total_time = time.time() - request._performance_start_time
            
            # Contar consultas realizadas
            total_queries = len(connection.queries) - getattr(request, '_performance_queries_start', 0)
            
            # Calcular tiempo de queries
            query_time = 0
            if hasattr(connection, 'queries'):
                for query in connection.queries[request._performance_queries_start:]:
                    query_time += float(query['time'])
            
            # Determinar si es lento
            is_slow = total_time > 1.0  # Más de 1 segundo es lento
            is_too_many_queries = total_queries > 10  # Más de 10 queries es sospechoso
            
            # Log de performance
            log_level = logging.WARNING if (is_slow or is_too_many_queries) else logging.INFO
            
            # Información del request
            path = request.get_full_path()
            method = request.method
            user = getattr(request, 'user', 'Anonymous')
            
            # Log principal
            logger.log(
                log_level,
                f"PERFORMANCE: {method} {path} | "
                f"Time: {total_time:.3f}s | "
                f"Queries: {total_queries} ({query_time:.3f}s) | "
                f"User: {user}"
            )
            
            # Log detallado para requests lentos
            if is_slow or is_too_many_queries:
                logger.warning(f"🐌 SLOW REQUEST DETECTED:")
                logger.warning(f"   Path: {path}")
                logger.warning(f"   Total Time: {total_time:.3f}s")
                logger.warning(f"   Database Queries: {total_queries}")
                logger.warning(f"   Database Time: {query_time:.3f}s")
                logger.warning(f"   Non-DB Time: {(total_time - query_time):.3f}s")
                
                # Mostrar queries más lentas
                if hasattr(connection, 'queries'):
                    slow_queries = [
                        q for q in connection.queries[request._performance_queries_start:]
                        if float(q['time']) > 0.1  # Queries que toman más de 100ms
                    ]
                    
                    for i, query in enumerate(slow_queries[:5]):  # Top 5
                        logger.warning(f"   Slow Query #{i+1}: {float(query['time']):.3f}s")
                        logger.warning(f"      SQL: {query['sql'][:200]}...")
            
            # Agregar headers de debug en desarrollo
            if settings.DEBUG:
                response['X-DB-Queries'] = str(total_queries)
                response['X-DB-Time'] = f"{query_time:.3f}s"
                response['X-Total-Time'] = f"{total_time:.3f}s"
        
        return response


class DatabaseQueryDebugMiddleware(MiddlewareMixin):
    """
    Middleware adicional para detectar patrones problemáticos
    """
    
    def process_response(self, request, response):
        if not settings.DEBUG:
            return response
            
        if hasattr(connection, 'queries') and len(connection.queries) > 0:
            # Detectar consultas duplicadas (problema N+1)
            sql_patterns = {}
            for query in connection.queries:
                # Normalizar query para detectar patrones
                normalized = self._normalize_sql(query['sql'])
                if normalized in sql_patterns:
                    sql_patterns[normalized] += 1
                else:
                    sql_patterns[normalized] = 1
            
            # Reportar consultas duplicadas
            duplicates = {sql: count for sql, count in sql_patterns.items() if count > 3}
            if duplicates:
                logger.warning("🔄 DUPLICATE QUERIES DETECTED (possible N+1 problem):")
                for sql, count in duplicates.items():
                    logger.warning(f"   {count}x: {sql[:150]}...")
        
        return response
    
    def _normalize_sql(self, sql):
        """Normaliza SQL para detectar patrones"""
        import re
        # Remover números y strings para detectar patrones
        normalized = re.sub(r'\d+', 'N', sql)
        normalized = re.sub(r"'[^']*'", "'S'", normalized)
        normalized = re.sub(r'"[^"]*"', '"S"', normalized)
        return normalized


def analyze_template_performance():
    """
    Función para analizar qué templates son más lentos de renderizar
    """
    print("🔍 ANALIZANDO TEMPLATES LENTOS...")
    
    # Esta función se puede expandir para detectar templates problemáticos
    slow_templates = [
        "home/panel_cubo.html",
        "includes/database_selector.html", 
        "base.html"
    ]
    
    print("📋 Templates que pueden estar causando lentitud:")
    for template in slow_templates:
        print(f"   - {template}")
    
    print("\n💡 RECOMENDACIONES:")
    print("1. Usar {% load cache %} y {% cache %} en templates")
    print("2. Minimizar loops {% for %} en templates")
    print("3. Usar {% include %} con 'only' para limitar contexto")
    print("4. Evitar consultas dentro de templates")
