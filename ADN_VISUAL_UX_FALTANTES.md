# ADN Visual y UX — Módulo Faltantes (Panel Bimbo)

Este documento sistematiza el diseño existente del módulo **Faltantes** para uso como referencia madre dentro del Panel Bimbo. **No incluye código ni lógica de negocio**; describe únicamente la capa visual/UX observada en los templates y estilos actuales.

---

## 1️⃣ Design Tokens (UI Variables)

> Base técnica observada: Bootstrap 5.3.2 (CDN) + utilidades (`text-*`, `bg-*`, `border-*`, `shadow*`) + componentes propios (modal simple con overlay).

### 1.1 Paleta principal (con HEX)

| Token sugerido | Uso en UI (observado) | Bootstrap/HEX (referencia) |
|---|---|---|
| `--bimbo-danger` | Titulares y énfasis crítico (H1, headers, valores faltantes, CTA principal) | `#dc3545` |
| `--bimbo-dark` | Sidebar y top-nav (`bg-dark`) | `#212529` |
| `--bimbo-white` | Headers de card / superficies (`bg-white`) | `#ffffff` |
| `--bimbo-light` | Encabezados de tabla / fondos suaves (`table-light`) | `#f8f9fa` |
| `--bimbo-muted` | Ayudas, hints y microcopy (`text-muted`) | `#6c757d` (aprox. por `--bs-secondary-color`) |

Nota: el módulo depende de los tokens por defecto de Bootstrap 5.3.2. Si existieran overrides globales (CSS adicional), los HEX podrían variar.

### 1.2 Colores semánticos

| Semántica | Token sugerido | Uso observado en Faltantes |
|---|---|---|
| **Primary** | `--bimbo-primary` (`#0d6efd`) | Badge resumen del filtro (`bg-primary`) y tarjetas KPI con borde `border-primary` |
| **Secondary** | `--bimbo-secondary` (`#6c757d`) | Badge período (`bg-secondary`), fila de totales en tabla (`bg-secondary`) |
| **Success** | `--bimbo-success` (`#198754`) | KPI Unidades, badge total (`bg-success`), botón de descarga (`btn-success`), estado “éxito” (`alert-success`) |
| **Info** | `--bimbo-info` (`#0dcaf0`) | KPI Nivel de servicio (`text-info`), badge agente (`bg-info text-dark`), estado informativo (`alert-info`) |
| **Danger** | `--bimbo-danger` (`#dc3545`) | CTA “Generar reporte” (`btn-danger`), modal spinner/progress, valores faltantes en tabla |
| **Warning** | `--bimbo-warning` (Bootstrap default `#ffc107`) | Estado “terminó sin resultados” (`alert-warning`) |

### 1.3 Fondos, bordes, sombras

| Token sugerido | Valor (referencia Bootstrap) | Dónde se ve |
|---|---|---|
| `--bimbo-surface` | `#ffffff` | `card-header bg-white`, superficies limpias |
| `--bimbo-surface-muted` | `#f8f9fa` | `table-light` (thead/tfoot) |
| `--bimbo-border` | `#dee2e6` | `border-bottom` (headers) y bordes Bootstrap por defecto |
| `--bimbo-shadow-sm` | `0 .125rem .25rem rgba(0,0,0,.075)` | Cards principales (`shadow-sm`) |
| `--bimbo-shadow` | `0 .5rem 1rem rgba(0,0,0,.15)` | KPI cards (`shadow`) y modal content (`shadow`) |
| `--bimbo-kpi-border-width` | `4px` | KPI cards con `border-start border-4` |
| `--bimbo-modal-overlay` | `rgba(0,0,0,0.6)` | Overlay del modal de procesamiento (`#processingModal`) |

### 1.4 Estados (hover, disabled, active)

| Estado | Regla observada | Ejemplos |
|---|---|---|
| **Hover** | Se apoya en Bootstrap | `table-hover` resalta filas al pasar el mouse; botones siguen estilos Bootstrap |
| **Disabled** | Se usa `disabled` en controles | `filter_value_select` inicia deshabilitado; `ceves_code_select` deshabilitado si no hay catálogo; botón “Generar reporte” se deshabilita en ejecución |
| **Active/Selected** | `btn-check` + `btn btn-outline-secondary` | Radio group “Tipo de filtro” como botones segmentados |
| **Sticky** | `sticky-top` / `sticky-bottom` | Header y footer de tabla se mantienen visibles en scroll |

---

## 2️⃣ Jerarquía Tipográfica

> La fuente declarada a nivel base es **Nunito** (Google Fonts) con pesos 400/600/700. En el proyecto existe además `panel.css` que declara Roboto para `body`; en el módulo Faltantes no se observa un override tipográfico propio dentro del template.

### 2.1 Fuente base

- `--bimbo-font-family-base`: `"Nunito", system-ui, -apple-system, "Segoe UI", Arial, sans-serif`

### 2.2 Pesos usados

- `--bimbo-font-weight-regular`: 400
- `--bimbo-font-weight-semibold`: 600
- `--bimbo-font-weight-bold`: 700

### 2.3 Tamaños/jerarquías (por contexto)

- **Título principal (H1)**: clase `h3` + `text-danger` + `mb-1`.
- **Subtítulo / descripción**: `text-muted small`.
- **Títulos de sección (cards/headers)**: `h6`/`h5` + `fw-bold` + `text-danger`.
- **KPIs**:
  - Label: `text-secondary small fw-bold text-uppercase`.
  - Valor: `h4 mb-0 fw-bold` + color semántico (`text-danger`, `text-success`, `text-info`).
- **Tabla**:
  - Densidad: `table-sm small`.
  - Énfasis: `fw-bold text-dark` (producto), `fw-bold text-danger` (faltantes).
- **Microcopy**: `small text-muted` (hints, ayudas, “Aumenta si el conjunto es pesado”).

---

## 3️⃣ Layout Canónico del Módulo

> Layout desktop-first dentro de `container-fluid`, con sidebar institucional y contenido principal a la derecha.

### 3.1 Bloques reutilizables (orden visual)

1. **Top/Nav institucional** (global del layout): barra superior `navbar navbar-dark bg-dark`.
2. **Sidebar Panel Bimbo**: navegación vertical `bg-dark`, logo, usuario y links.
3. **Header del módulo (título + descripción)**:
   - Título en rojo (`text-danger`).
   - Microcopy en gris (`text-muted`).
4. **Barra de resumen (badges)**: fila compacta de badges con período, CEVE, filtro, filas.
5. **Card de filtros (control principal)**:
   - Header con título y ayuda.
   - Acciones a la derecha: “Limpiar” (secundario), “Generar reporte” (primario rojo).
   - Body con grid responsive (`row g-3`).
6. **Barra de estado / alert**: `alert` persistente que comunica qué hacer/qué pasa.
7. **Resultados (dashboard)** — contenedor oculto inicialmente:
   - Fila de **KPIs** (4 cards).
   - Card con **tabla de detalle**.
   - **CTA de descarga** alineado a la derecha.
8. **Empty state** (vacío inicial): mensaje centrado con icono.
9. **Modal de procesamiento**: overlay + card oscura con progress bar roja.

### 3.2 Jerarquía y relación control → resultado

- La pantalla separa claramente:
  - **Control**: card de filtros + acciones.
  - **Feedback**: alert de estado + modal durante procesamiento.
  - **Resultado**: KPIs (resumen) + tabla (detalle) + descarga (acción final).

### 3.3 Responsividad

- Grid basado en Bootstrap (`col-md-*`): filtros y KPIs se acomodan por columnas en desktop.
- Tabla contenida en `table-responsive` con `max-height` y scroll vertical para mantener el layout estable.

---

## 4️⃣ Catálogo de Componentes UI

### 4.1 Selectores / Entradas

**A) Selector de Empresa (global)**
- **Propósito**: seleccionar “empresa” (base) en la suite.
- **UI**: wrapper `bg-dark` + `<select id="database_select">`.
- **Estado visual**: institucional oscuro; control estándar.
- **Reglas de uso**:
  - Debe estar definido antes de cargar catálogos (proveedores/categorías/productos).
  - Se persiste en `sessionStorage`.

**B) Selector CEVE (obligatorio)**
- **Propósito**: seleccionar la agencia/CEVE.
- **UI**: `form-select form-select-sm`.
- **Estados**:
  - Disabled si no hay `ceves_catalog`.
  - Required.

**C) Rango de Fechas**
- **Propósito**: parametrizar ventana temporal del reporte.
- **UI**: daterangepicker sobre contenedor `#reportrange` con iconografía.
- **Feedback**: actualiza inputs ocultos `IdtReporteIni/IdtReporteFin`.

**D) Tipo de filtro (radio group estilo botones)**
- **Propósito**: elegir dimensión de filtrado (proveedor/categoría/subcategoría/producto).
- **UI**: `btn-check` + `btn btn-outline-secondary btn-sm`.
- **Regla clave**: por defecto selecciona **proveedor**.

**E) Valor del filtro (catálogo dinámico)**
- **Propósito**: elegir el valor concreto del catálogo según tipo.
- **UI**: `form-select form-select-sm`.
- **Estados**:
  - Inicia disabled.
  - Muestra placeholders de “Cargando…”, “Sin opciones”, “Error cargando catálogo”.

**F) Tamaño de lote**
- **Propósito**: control de performance percibida.
- **UI**: select pequeño (`form-select-sm`) con opciones 10k–100k.

### 4.2 Botones

**A) Primario: “Generar reporte”**
- **Clase**: `btn btn-danger btn-sm` + icono play.
- **Semántica**: acción principal de ejecución.
- **Estados**:
  - Disabled durante procesamiento.

**B) Secundario: “Limpiar”**
- **Clase**: `btn btn-outline-secondary btn-sm`.
- **Semántica**: reset de filtros + feedback informativo.

**C) Descarga**
- **Clase**: `btn btn-success`.
- **Semántica**: acción final post-éxito.
- **Regla**: se actualiza el `href` al archivo generado.

### 4.3 KPIs

- **Estructura**: cards con borde izquierdo de 4px (`border-start border-4`) + `shadow`.
- **Semántica por color**:
  - Total faltante: rojo.
  - Unidades faltantes: verde.
  - Nivel de servicio: info (cian).
  - Top producto: primary (azul) en el borde.

### 4.4 Tabla de detalle

- **Densidad**: `table table-hover table-sm small`.
- **Jerarquía**:
  - Producto: `fw-bold text-dark` + truncado.
  - Cantidad faltante y valor faltante: `fw-bold text-danger`.
- **Usabilidad**:
  - Header sticky (`sticky-top`) y footer sticky (`sticky-bottom`).
  - Scroll vertical controlado (`max-height: 550px`).
  - Totales generales en footer con `bg-secondary` y texto blanco.

### 4.5 Modal de procesamiento

- **Propósito**: bloquear interacción y confirmar progreso.
- **UI**:
  - Overlay `rgba(0,0,0,0.6)`.
  - Card oscura `bg-dark text-white`.
  - Spinner rojo (`fa-spinner ... text-danger`) + progressbar roja.
  - Timer de segundos.

### 4.6 Alerts y badges

- **Alert persistente**: `#statusMessage` cambia de `alert-info` a `alert-success/alert-danger/alert-warning`.
- **Badges resumen**: `#fc-summary` muestra periodo, CEVE, filtro y cantidad de filas (colores: secondary/info/primary/success).

---

## 5️⃣ Flujo UX Operativo

1. **Configuración**
   - Ve: card de filtros + hints.
   - Hace: selecciona Empresa → CEVE → fechas → tipo de filtro → valor → lote.

2. **Validación**
   - Ve: si falta algo, aparece `alert()` con mensaje específico (empresa/ceve/fechas/filtro).
   - Hace: corrige campos.

3. **Ejecución**
   - Acción: click en “Generar reporte”.
   - Feedback inmediato: aparece modal y progress arranca (“Solicitando ejecución…”).

4. **Espera**
   - Ve: modal con spinner + barra + etapa (stage) + contador de tiempo.
   - Bloqueo: el botón de ejecutar queda disabled.

5. **Análisis**
   - Ve: KPIs + tabla de detalle.
   - Feedback: alert “Reporte generado correctamente.”
   - Contexto: badges resumen con período/CEVE/filtro/filas.

6. **Descarga**
   - Ve: CTA “Descargar archivo” alineado a la derecha.
   - Hace: descarga del Excel generado.

---

## 6️⃣ Estados del Sistema (State Machine Visual)

> Elementos clave de estado: `#processingModal`, `#dashboardResults`, `#emptyState`, `#statusMessage`, `#fc-summary`, botón `#submitBtnFaltantes`.

### Estado A — Inicial / vacío

- **Se muestra**: `#statusMessage` (info: “Ejecuta el reporte…”), `#emptyState`.
- **Se oculta**: `#dashboardResults`, `#fc-summary`, `#processingModal`.
- **Se bloquea**: nada.
- **Color semántico**: `info`.

### Estado B — Loading / procesando

- **Se muestra**: `#processingModal` (overlay oscuro + progressbar roja).
- **Se oculta**: resultados permanecen según estado previo; el foco es el modal.
- **Se bloquea**: botón “Generar reporte” disabled.
- **Color semántico**: `danger` para progreso (barra/spinner) + `info` para status textual (stage).

### Estado C — Éxito

- **Se muestra**: `#dashboardResults`, `#fc-summary`, CTA descarga.
- **Se oculta**: `#emptyState`, modal.
- **Se bloquea**: nada (se re-habilita el botón).
- **Color semántico**: `success` (alert) + colores KPI/tabla según métrica.

### Estado D — Error

- **Se muestra**: `#statusMessage` con `alert-danger` y texto “Falló: …” o “Error: …”.
- **Se oculta**: modal; `#dashboardResults` se oculta en caso de error crítico.
- **Se bloquea**: se re-habilita ejecutar.
- **Color semántico**: `danger`.

### Estado E — Sin datos (terminó sin resultados)

- **Se muestra**: `alert-warning` (“La tarea terminó sin resultados.”) o vacío con guía.
- **Se oculta**: resultados si no hay payload; `#emptyState` se usa como guía.
- **Se bloquea**: no.
- **Color semántico**: `warning`.

---

## 7️⃣ Principios de Diseño Extraídos

- Rojo (`danger`) reservado para identidad y criticidad: título del módulo, CTA primario y métricas críticas.
- Separación fuerte Control → Resultado: filtros en card independiente; KPIs+tabla aparecen solo tras ejecución.
- Feedback continuo del sistema: alert persistente + modal de procesamiento + progreso por etapas.
- Resumen compacto antes del detalle: badges (contexto) → KPIs (resumen) → tabla (detalle).
- Densidad informativa controlada: `table-sm` + scroll vertical + sticky header/footer + totales fijos.
- Semántica de color consistente: success=descarga/éxito, info=estado/servicio, secondary=contexto/totales.
- Patrón institucional coherente: navegación oscura (sidebar/top-nav) como marco constante.

---

## 8️⃣ Checklist de Reutilización

Usa esta lista para validar si otro reporte está alineado con el ADN de Faltantes.

### Identidad y tokens
- [ ] Usa Bootstrap 5.3.x como base semántica (`danger/success/info/primary/secondary`).
- [ ] Emplea `danger` solo para: título del módulo, CTA primario y métricas críticas.
- [ ] Mantiene marco institucional oscuro (`bg-dark`) en sidebar/top-nav.

### Tipografía y jerarquía
- [ ] Fuente base declarada y consistente (Nunito) con pesos 400/600/700.
- [ ] H1 del módulo con jerarquía tipo `h3` y color `text-danger`.
- [ ] Labels y microcopy usan `small` + `text-muted`.

### Layout
- [ ] Orden canónico: Header → Badges resumen → Card filtros → Alert estado → KPIs → Tabla → Descarga.
- [ ] KPIs en cards con borde izquierdo semántico (`border-start border-4`).
- [ ] Tabla con densidad `table-sm` y soporte de scroll con header/footer sticky.

### Componentes
- [ ] Filtros en card con acciones a la derecha: secundario (outline) + primario (danger).
- [ ] Estado persistente con `alert` que cambia por semántica.
- [ ] Modal de procesamiento bloqueante con progreso y etapa.

### UX operativo
- [ ] Validación previa a ejecución (mensajes claros y específicos).
- [ ] Durante ejecución: botón disabled + modal visible + progreso/etapas.
- [ ] Post-éxito: KPIs + tabla + CTA descarga visibles y coherentes.

### Estados del sistema
- [ ] Inicial: guía clara + empty state.
- [ ] Loading: modal overlay visible.
- [ ] Éxito: `alert-success` + resultados visibles.
- [ ] Error: `alert-danger` + recuperación (re-ejecutar sin recargar).
- [ ] Sin datos: `alert-warning` + guía.
