"""
diagnostico_login.py
====================
Script de diagnóstico para inspeccionar la ventana de login de Microsoft.

INSTRUCCIONES:
1. Ejecuta este script MIENTRAS la ventana de "Pick an account" / "Seleccionar cuenta" 
   de Microsoft esté visible en pantalla.
2. El script intentará encontrarla y volcará TODOS los controles disponibles en consola.
3. También intentará hacer clic usando múltiples estrategias y reportará cuál funcionó.

Ejecución:
    python scripts/extrae_bi/diagnostico_login.py
"""

import time
import re
import sys
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

try:
    from pywinauto.application import Application
    from pywinauto import Desktop
    import pywinauto.timings
except ImportError:
    logging.error("pywinauto no está instalado. Instala con: pip install pywinauto")
    sys.exit(1)


# ==============================================================================
# Configuración
# ==============================================================================
MAX_WAIT_SEC = 60  # segundos máximos esperando la ventana
POLL_INTERVAL = 2  # segundos entre intentos

# Patrones de título que puede tener la ventana de login de Microsoft
WINDOW_PATTERNS = [
    ".*Pick an account.*",
    ".*Seleccionar.*cuenta.*",
    ".*Elige una cuenta.*",
    ".*Iniciar sesión.*",
    ".*Sign in.*",
    ".*Microsoft.*",
    ".*Account.*",
    ".*Office.*",
    ".*Excel.*",
]

# Textos a ignorar
SKIP_TEXTS = {
    "Atrás", "Back", "Siguiente", "Next", "Cancelar", "Cancel",
    "Más opciones", "More options", "Usar otra cuenta",
    "Use another account", "", "X", "Pick an account",
    "Selecciona una cuenta", "Sign in", "Iniciar sesión",
    "Elige una cuenta", "Choose an account",
}


# ==============================================================================
# Utilidades
# ==============================================================================
def dump_all_controls(dialog, label=""):
    """Vuelca todos los controles de un diálogo, con tipo, nombre y texto."""
    print(f"\n{'='*70}")
    print(f"  CONTROLES DE: {label or dialog}")
    print(f"{'='*70}")
    try:
        descendants = dialog.descendants()
        for i, ctrl in enumerate(descendants):
            try:
                ctrl_type = ctrl.element_info.control_type
                ctrl_name = ctrl.element_info.name or ""
                ctrl_text = ""
                try:
                    ctrl_text = ctrl.window_text() or ""
                except Exception:
                    pass
                is_enabled = ""
                try:
                    is_enabled = "ENABLED" if ctrl.is_enabled() else "disabled"
                except Exception:
                    pass
                print(
                    f"  [{i:03d}] tipo={ctrl_type:<20} name='{ctrl_name[:40]:<40}' "
                    f"text='{ctrl_text[:50]:<50}' {is_enabled}"
                )
            except Exception as e:
                print(f"  [{i:03d}] ERROR al leer control: {e}")
    except Exception as e:
        print(f"  ERROR al obtener descendants: {e}")
    print(f"{'='*70}\n")


def try_click(ctrl, label=""):
    """Intenta hacer clic en un control usando varios métodos."""
    methods = [
        ("click_input", lambda c: c.click_input()),
        ("invoke",      lambda c: c.invoke()),
        ("set_focus+click_input", lambda c: (c.set_focus(), c.click_input())),
        ("parent.click_input", lambda c: c.parent().click_input()),
        ("parent.invoke",      lambda c: c.parent().invoke()),
    ]
    for method_name, method_fn in methods:
        try:
            method_fn(ctrl)
            logging.info(f"  ✅ CLIC EXITOSO con método '{method_name}' en: {label}")
            return True
        except Exception as e:
            logging.debug(f"  ❌ método '{method_name}' falló en {label}: {e}")
    return False


# ==============================================================================
# Búsqueda y diagnóstico de ventana
# ==============================================================================
def find_login_window():
    """Busca la ventana de login de Microsoft usando todos los patrones."""
    # Intento 1: con Application().connect
    for pattern in WINDOW_PATTERNS:
        try:
            app = Application(backend="uia").connect(title_re=pattern, timeout=1)
            dialog = app.window(title_re=pattern)
            dialog.wait("visible", timeout=1)
            logging.info(f"[connect] Ventana encontrada: patrón='{pattern}'")
            return dialog, pattern
        except Exception:
            pass

    # Intento 2: Desktop enumerar todas las ventanas
    try:
        desktop = Desktop(backend="uia")
        all_wins = desktop.windows()
        for win in all_wins:
            try:
                title = win.window_text()
                if title:
                    for pattern in WINDOW_PATTERNS:
                        if re.match(pattern, title, re.IGNORECASE):
                            logging.info(f"[Desktop] Ventana encontrada: '{title}'  patrón='{pattern}'")
                            return win, title
            except Exception:
                pass
    except Exception as e:
        logging.warning(f"[Desktop] Error al enumerar ventanas: {e}")

    return None, None


def main():
    logging.info("=" * 70)
    logging.info("  DIAGNÓSTICO DE VENTANA DE LOGIN MICROSOFT")
    logging.info("=" * 70)
    logging.info(f"Esperando hasta {MAX_WAIT_SEC}s que aparezca la ventana de login...")
    logging.info("Abre Excel y espera a que aparezca el diálogo de cuenta Microsoft.")
    logging.info("=" * 70)

    waited = 0
    dialog = None
    found_pattern = ""

    while waited < MAX_WAIT_SEC:
        dialog, found_pattern = find_login_window()
        if dialog:
            break
        logging.info(f"Ventana no encontrada, reintentando en {POLL_INTERVAL}s... ({waited}s/{MAX_WAIT_SEC}s)")
        time.sleep(POLL_INTERVAL)
        waited += POLL_INTERVAL

    if not dialog:
        logging.error(f"No se encontró ninguna ventana de login tras {MAX_WAIT_SEC}s.")
        logging.error("Asegúrate de que la ventana esté visible mientras corres este script.")
        sys.exit(1)

    # ── Volcado completo de controles ─────────────────────────────────────────
    dump_all_controls(dialog, label=f"'{found_pattern}'")

    # ── Intentar encontrar y hacer clic en el email / cuenta ─────────────────
    logging.info("\nIntentando seleccionar la cuenta de usuario...")

    clicked = False

    # Estrategia A: cualquier control cuyo texto tenga '@'
    logging.info("--- Estrategia A: buscar '@' en texto ---")
    try:
        for ctrl in dialog.descendants():
            ctrl_text = ""
            try:
                ctrl_text = ctrl.window_text() or ""
            except Exception:
                pass
            if "@" in ctrl_text and ctrl_text not in SKIP_TEXTS:
                logging.info(f"  Candidato A: '{ctrl_text}' tipo={ctrl.element_info.control_type}")
                if try_click(ctrl, label=ctrl_text):
                    clicked = True
                    break
    except Exception as e:
        logging.warning(f"  Estrategia A error: {e}")

    # Estrategia B: ListItem o DataItem (cualquiera que no sea genérico)
    if not clicked:
        logging.info("--- Estrategia B: ListItem / DataItem / Button ---")
        for ctrl_type in ["ListItem", "DataItem", "Button", "ListBoxItem"]:
            try:
                controls = dialog.descendants(control_type=ctrl_type)
                for ctrl in controls:
                    ctrl_text = ""
                    try:
                        ctrl_text = ctrl.window_text() or ""
                    except Exception:
                        pass
                    if ctrl_text and ctrl_text not in SKIP_TEXTS:
                        logging.info(f"  Candidato B: '{ctrl_text}' tipo={ctrl_type}")
                        if try_click(ctrl, label=ctrl_text):
                            clicked = True
                            break
                if clicked:
                    break
            except Exception as e:
                logging.warning(f"  Estrategia B tipo={ctrl_type} error: {e}")

    # Estrategia C: usar pywinauto print_control_identifiers para ver IDs usables
    logging.info("\n--- Dump detallado con print_control_identifiers ---")
    try:
        dialog.print_control_identifiers()
    except Exception as e:
        logging.warning(f"  print_control_identifiers error: {e}")

    if clicked:
        logging.info("\n✅ CLIC REALIZADO EXITOSAMENTE.")
    else:
        logging.error("\n❌ NO se pudo hacer clic. Revisa el volcado de controles arriba.")
        logging.error("   Busca en el volcado el control que corresponde a tu cuenta")
        logging.error("   y anota su 'tipo' y 'text' para ajustar el código.")


if __name__ == "__main__":
    main()
