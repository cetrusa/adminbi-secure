import win32com.client
import os
import time
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.utils import COMMASPACE
from email import encoders
import smtplib
import datetime
import markdown
import logging
from dateutil.relativedelta import relativedelta
from scripts.conexion import Conexion as con
from scripts.config import ConfigBasic
from sqlalchemy import create_engine, text

from pywinauto.application import Application
from pywinauto import Desktop
import pywinauto
import threading
import re
import ctypes


class DataBaseConnection:
    def __init__(self, config, mysql_engine=None, sqlite_engine=None):
        self.config = config
        # Asegurarse de que los engines son instancias de conexión válidas y no cadenas
        self.engine_mysql_bi = (
            mysql_engine if mysql_engine else self.create_engine_mysql_bi()
        )
        self.engine_mysql_out = (
            mysql_engine if mysql_engine else self.create_engine_mysql_out()
        )
        self.engine_sqlite = (
            sqlite_engine if sqlite_engine else create_engine("sqlite:///mydata.db")
        )
        # print(self.engine_sqlite)

    def create_engine_mysql_bi(self):
        # Simplificación en la obtención de los parámetros de configuración
        user, password, host, port, database = (
            self.config.get("nmUsrIn"),
            self.config.get("txPassIn"),
            self.config.get("hostServerIn"),
            self.config.get("portServerIn"),
            self.config.get("dbBi"),
        )
        return con.ConexionMariadb3(
            str(user), str(password), str(host), int(port), str(database)
        )

    def create_engine_mysql_out(self):
        # Simplificación en la obtención de los parámetros de configuración
        user, password, host, port, database = (
            self.config.get("nmUsrOut"),
            self.config.get("txPassOut"),
            self.config.get("hostServerOut"),
            self.config.get("portServerOut"),
            self.config.get("dbSidis"),
        )
        return con.ConexionMariadb3(
            str(user), str(password), str(host), int(port), str(database)
        )


class CompiUpdate:
    def wait_until_excel_ready(self, excel, timeout=120):
        """
        Espera a que Excel esté listo (no ocupado ni calculando) antes de continuar.
        """
        start = time.time()
        while True:
            try:
                # CalculationState: 0=xlDone, 1=xlCalculating, 2=xlPending
                calc_state = getattr(excel, "CalculationState", 0)
                ready = getattr(excel, "Ready", True)
                if calc_state == 0 and ready:
                    logging.info(
                        f"[EXCEL][WAIT] Excel listo para continuar (CalculationState={calc_state}, Ready={ready})"
                    )
                    break
                else:
                    logging.info(
                        f"[EXCEL][WAIT] Esperando a que Excel termine... (CalculationState={calc_state}, Ready={ready})"
                    )
            except Exception as e:
                logging.info(
                    f"[EXCEL][WAIT] Excepción al chequear estado de Excel: {e}"
                )
            if time.time() - start > timeout:
                logging.error(
                    f"[EXCEL][WAIT] Timeout esperando a que Excel esté listo tras {timeout} segundos."
                )
                break
            time.sleep(2)

    def __init__(self, database_name):
        self.timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        self.file_path = None
        self.local_copy_path = None
        self.setup_logging()
        self.configurar(database_name)

    def configurar(self, database_name):
        try:
            self.config_basic = ConfigBasic(database_name)
            self.config = self.config_basic.config
            # config_basic.print_configuration()
            # print(self.config.get("txProcedureExtrae", []))
            self.db_connection = DataBaseConnection(config=self.config)
            self.engine_sqlite = self.db_connection.engine_sqlite
            self.engine_mysql_bi = self.db_connection.engine_mysql_bi
            self.engine_mysql_out = self.db_connection.engine_mysql_out
            # print("Configuraciones preliminares de actualización terminadas")
        except Exception as e:
            logging.error(f"Error al inicializar Actualización: {e}")
            raise

    def setup_logging(self):
        import sys
        # Ruta del log junto al ejecutable (funciona tanto en .py como en .exe)
        base_dir = os.path.dirname(os.path.abspath(sys.executable if getattr(sys, 'frozen', False) else __file__))
        log_path = os.path.join(base_dir, "process.log")
        logging.basicConfig(
            level=logging.INFO,
            filename=log_path,
            filemode="w",
            format="%(asctime)s %(levelname)s %(message)s",
            force=True,
        )
        logging.info(f"Log iniciado en: {log_path}")


    def find_file(self, file_paths):
        for path in file_paths:
            if os.path.exists(path):
                logging.info(f"Archivo encontrado exitosamente en: {path}")
                return path
        logging.error("No se pudo encontrar el archivo.")
        return None

    def list_slicer_names(self):
        excel = win32com.client.Dispatch("Excel.Application")
        try:
            excel.Visible = True
            workbook = excel.Workbooks.Open(self.file_path)
            slicer_caches = workbook.SlicerCaches
            for slicer_cache in slicer_caches:
                print(f"Slicer Cache Name: {slicer_cache.Name}")
        except Exception as e:
            print(f"Error accessing SlicerCaches: {e}")
        finally:
            workbook.Close(SaveChanges=False)
            excel.Quit()

    def refresh_excel(self):
        logging.info("Inicia Proceso de actualización de Excel")
        self.file_path = self.find_file(
            [
                "G:\\OneDrive\\OneDrive - Asistencia Movil SAS\\Compi_bi.xlsx",
                "C:\\OneDrive - Asistencia Movil SAS\\Compi_bi.xlsx",
                "D:\\OneDrive - Asistencia Movil SAS\\Compi_bi.xlsx",
            ]
        )
        print(self.local_copy_path)
        if self.file_path:
            self.local_copy_path = (
                f"{self.file_path[0]}:\\Powerbi\\Compi\\Compi_bi_{self.timestamp}.xlsx"
            )
            print(self.local_copy_path)
            logging.info(f"La ruta para la copia local será: {self.local_copy_path}")
            self._open_and_process_excel()
        else:
            logging.error("Proceso terminado debido a la falta del archivo requerido.")

    def _find_window_hwnd(self, title_patterns):
        """
        Busca la ventana del diálogo de login por título Y por clase de ventana.
        Retorna (hwnd, title) si la encuentra, o (None, None).
        """
        import win32gui
        import re

        # Clases de ventana que usa Microsoft para el diálogo de cuenta
        # (WebView2 usa Chrome_WidgetWin_1, el shell puede ser diferentes clases)
        TARGET_CLASSES = {
            "Chrome_WidgetWin_1",     # WebView2 / Edge embedded
            "#32770",                  # Dialog box estándar Win32
            "NativeHWNDHost",          # Host de WebView2 en aplicaciones nativas
        }

        found = []
        appframe_candidates = []  # Fallback: ApplicationFrameWindow por tamaño

        def enum_cb(hwnd, _):
            if not win32gui.IsWindowVisible(hwnd):
                return
            title = win32gui.GetWindowText(hwnd) or ""
            wclass = win32gui.GetClassName(hwnd) or ""

            # Buscar por título
            for pattern in title_patterns:
                if re.search(pattern, title, re.IGNORECASE):
                    found.append((hwnd, title, wclass, "title_match"))
                    return

            # Buscar por clase (WebView2 puede tener título vacío o no coincidente)
            if wclass in TARGET_CLASSES and title in ("", "Default IME"):
                # Comprobar si algún hijo tiene el título buscado
                try:
                    child_titles = []
                    def child_cb(child_hwnd, _):
                        ct = win32gui.GetWindowText(child_hwnd) or ""
                        if ct:
                            child_titles.append(ct)
                    win32gui.EnumChildWindows(hwnd, child_cb, None)
                    for ct in child_titles:
                        for pattern in title_patterns:
                            if re.search(pattern, ct, re.IGNORECASE):
                                found.append((hwnd, ct, wclass, "child_title_match"))
                                return
                except Exception:
                    pass

            # Fallback especial: ApplicationFrameWindow con título vacío y tamaño razonable.
            # El diálogo "Pick an account" de Microsoft (WebView2 embebido en UWP) aparece
            # como ApplicationFrameWindow sin título. El contenido WebView2 NO expone títulos
            # de hijos via EnumChildWindows, por lo que usamos heurística de tamaño.
            if wclass == "ApplicationFrameWindow" and not title:
                try:
                    rect = win32gui.GetWindowRect(hwnd)
                    w = rect[2] - rect[0]
                    h = rect[3] - rect[1]
                    if w > 300 and h > 300:
                        appframe_candidates.append((hwnd, "", wclass, "appframe_size_heuristic"))
                except Exception:
                    pass

        try:
            win32gui.EnumWindows(enum_cb, None)
        except Exception as e:
            logging.warning(f"[LOGIN][find] EnumWindows error: {e}")

        if found:
            hwnd, title, wclass, match_type = found[0]
            logging.info(
                f"[LOGIN][find] hwnd={hwnd} title='{title}' "
                f"class='{wclass}' match={match_type}"
            )
            return hwnd, title

        # Fallback: usar el ApplicationFrameWindow de mayor área si no encontramos por título
        if appframe_candidates:
            def _area(c):
                try:
                    r = win32gui.GetWindowRect(c[0])
                    return (r[2] - r[0]) * (r[3] - r[1])
                except Exception:
                    return 0
            appframe_candidates.sort(key=_area, reverse=True)
            hwnd, title, wclass, match_type = appframe_candidates[0]
            logging.info(
                f"[LOGIN][find] FALLBACK hwnd={hwnd} "
                f"class='{wclass}' match={match_type} "
                f"(ApplicationFrameWindow sin título, heurística de tamaño)"
            )
            return hwnd, title

        return None, None

    def _log_all_visible_windows(self):
        """Vuelca todas las ventanas visibles al log para diagnóstico."""
        import win32gui
        try:
            lines = []
            def cb(hwnd, _):
                if win32gui.IsWindowVisible(hwnd):
                    t = win32gui.GetWindowText(hwnd) or ""
                    c = win32gui.GetClassName(hwnd) or ""
                    r = win32gui.GetWindowRect(hwnd)
                    if t or c not in ("", "WorkerW", "Progman"):
                        lines.append(f"  hwnd={hwnd:<10} class={c:<30} rect={r} title='{t}'")
            win32gui.EnumWindows(cb, None)
            logging.info("[LOGIN][AllWindows]\n" + "\n".join(lines))
        except Exception as e:
            logging.warning(f"[LOGIN][AllWindows] Error: {e}")

    def handle_microsoft_login_window(self):
        """
        Monitorea y maneja automáticamente la ventana 'Pick an account' de Microsoft.
        Estrategias en orden de prioridad:
          A. SendInput teclado ENTER — el tile de cuenta está pre-enfocado.
          B. TAB + ENTER — mueve foco y confirma.
          C. pywinauto UIA — UI Automation nativa, funciona con WebView2.
          D. SendInput mouse (DPI-aware) — reemplaza el deprecated mouse_event.
          E. PostMessage a Chrome_WidgetWin_1 — último recurso.
        """
        import win32gui
        import win32con
        import win32api
        import ctypes

        logging.info("[LOGIN] Iniciando monitoreo de login 'Pick an account'...")

        max_wait = 180
        waited = 0

        # Patrones ESTRICTOS — sin 'Microsoft' genérico para evitar falsos positivos
        title_patterns = [
            r"Pick an account",
            r"Seleccionar.*cuenta",
            r"Elige.*cuenta",
            r"Iniciar sesi",
        ]

        # ── Estructuras para SendInput (teclado) ─────────────────────────────
        INPUT_KEYBOARD = 1
        INPUT_MOUSE = 0
        KEYEVENTF_KEYUP = 0x0002
        VK_RETURN = 0x0D
        VK_TAB = 0x09

        class KEYBDINPUT(ctypes.Structure):
            _fields_ = [
                ("wVk", ctypes.c_ushort), ("wScan", ctypes.c_ushort),
                ("dwFlags", ctypes.c_ulong), ("time", ctypes.c_ulong),
                ("dwExtraInfo", ctypes.POINTER(ctypes.c_ulong)),
            ]

        class MOUSEINPUT(ctypes.Structure):
            _fields_ = [
                ("dx", ctypes.c_long), ("dy", ctypes.c_long),
                ("mouseData", ctypes.c_ulong), ("dwFlags", ctypes.c_ulong),
                ("time", ctypes.c_ulong), ("dwExtraInfo", ctypes.POINTER(ctypes.c_ulong)),
            ]

        class _INPUT_UNION(ctypes.Union):
            _fields_ = [("ki", KEYBDINPUT), ("mi", MOUSEINPUT)]

        class INPUT(ctypes.Structure):
            _fields_ = [("type", ctypes.c_ulong), ("_u", _INPUT_UNION)]

        def send_key(vk):
            """Envía una tecla completa (down + up) vía SendInput."""
            inp_down = INPUT(type=INPUT_KEYBOARD)
            inp_down._u.ki.wVk = vk
            inp_up = INPUT(type=INPUT_KEYBOARD)
            inp_up._u.ki.wVk = vk
            inp_up._u.ki.dwFlags = KEYEVENTF_KEYUP
            ctypes.windll.user32.SendInput(1, ctypes.byref(inp_down), ctypes.sizeof(INPUT))
            time.sleep(0.05)
            ctypes.windll.user32.SendInput(1, ctypes.byref(inp_up), ctypes.sizeof(INPUT))

        def send_mouse_click(px, py):
            """Clic de mouse vía SendInput con coordenadas absolutas (0-65535)."""
            MOUSEEVENTF_MOVE = 0x0001
            MOUSEEVENTF_LEFTDOWN = 0x0002
            MOUSEEVENTF_LEFTUP = 0x0004
            MOUSEEVENTF_ABSOLUTE = 0x8000

            for flags, data in [
                (MOUSEEVENTF_MOVE | MOUSEEVENTF_ABSOLUTE, 0),
                (MOUSEEVENTF_LEFTDOWN | MOUSEEVENTF_ABSOLUTE, 0),
                (MOUSEEVENTF_LEFTUP | MOUSEEVENTF_ABSOLUTE, 0),
            ]:
                inp = INPUT(type=INPUT_MOUSE)
                inp._u.mi.dx = px
                inp._u.mi.dy = py
                inp._u.mi.dwFlags = flags
                ctypes.windll.user32.SendInput(1, ctypes.byref(inp), ctypes.sizeof(INPUT))
                time.sleep(0.05)

        # Dimensiones físicas del monitor para coordenadas absolutas DPI-aware
        SM_W = ctypes.windll.user32.GetSystemMetrics(0)
        SM_H = ctypes.windll.user32.GetSystemMetrics(1)

        while not self.login_window_handled and waited < max_wait:
            logging.info(f"[LOGIN] Buscando ventana ({waited}s/{max_wait}s)...")
            self._log_all_visible_windows()

            hwnd, win_title = self._find_window_hwnd(title_patterns)

            if hwnd is None:
                logging.info("[LOGIN] Ventana no encontrada. Reintentando en 3s...")
                time.sleep(3)
                waited += 3
                continue

            # ── Obtener rect de la ventana ────────────────────────────────────
            try:
                wx, wy, wx2, wy2 = win32gui.GetWindowRect(hwnd)
                ww, wh = wx2 - wx, wy2 - wy
                logging.info(f"[LOGIN] hwnd={hwnd} '{win_title}' rect=({wx},{wy})->({wx2},{wy2}) w={ww} h={wh}")
            except Exception as e:
                logging.warning(f"[LOGIN] GetWindowRect: {e}")
                time.sleep(3); waited += 3; continue

            # ── Traer al frente ───────────────────────────────────────────────
            # Buscar el hijo Chrome_WidgetWin_1 (WebView2) para enfocar directamente
            webview_hwnd = None
            try:
                def _find_webview(ch, _):
                    if webview_hwnd is None and win32gui.GetClassName(ch) in (
                        "Chrome_WidgetWin_1", "NativeHWNDHost"
                    ):
                        # Guardamos en lista mutable para acceder desde closure
                        _find_webview.result = ch
                _find_webview.result = None
                win32gui.EnumChildWindows(hwnd, _find_webview, None)
                webview_hwnd = _find_webview.result
                if webview_hwnd:
                    logging.info(f"[LOGIN] WebView2 child hwnd={webview_hwnd} encontrado dentro del contenedor.")
            except Exception as e:
                logging.warning(f"[LOGIN] Búsqueda WebView2 child: {e}")

            try:
                ctypes.windll.user32.AllowSetForegroundWindow(-1)
                # Enfocar primero el contenedor y luego el child WebView2 si existe
                win32gui.SetForegroundWindow(hwnd)
                time.sleep(0.3)
                if webview_hwnd:
                    try:
                        win32gui.SetForegroundWindow(webview_hwnd)
                        time.sleep(0.3)
                    except Exception:
                        pass
            except Exception:
                pass
            time.sleep(0.2)

            clicked = False

            # ── Estrategia A: SendInput ENTER (tile pre-enfocado) ─────────────
            logging.info("[LOGIN][A] Enviando ENTER via SendInput...")
            try:
                send_key(VK_RETURN)
                time.sleep(1.5)
                hwnd_check, _ = self._find_window_hwnd(title_patterns)
                if hwnd_check is None:
                    logging.info("[LOGIN][A] ✅ Ventana cerrada con ENTER.")
                    self.login_window_handled = True
                    clicked = True
            except Exception as e:
                logging.warning(f"[LOGIN][A] Error: {e}")

            # ── Estrategia B: TAB + ENTER ─────────────────────────────────────
            if not clicked:
                logging.info("[LOGIN][B] Enviando TAB+ENTER via SendInput...")
                try:
                    send_key(VK_TAB)
                    time.sleep(0.2)
                    send_key(VK_RETURN)
                    time.sleep(1.5)
                    hwnd_check, _ = self._find_window_hwnd(title_patterns)
                    if hwnd_check is None:
                        logging.info("[LOGIN][B] ✅ Ventana cerrada con TAB+ENTER.")
                        self.login_window_handled = True
                        clicked = True
                except Exception as e:
                    logging.warning(f"[LOGIN][B] Error: {e}")

            # ── Estrategia C: pywinauto UIA (UI Automation nativa) ────────────
            if not clicked:
                logging.info("[LOGIN][C] Intentando pywinauto UIA backend...")
                try:
                    # Conectar directamente por hwnd — el diálogo no expone título
                    app = Application(backend='uia').connect(handle=hwnd)
                    dlg = app.top_window()
                    logging.info(f"[LOGIN][C] Conectado por hwnd={hwnd}: {dlg}")
                    # Intentar hacer clic en el primer elemento de lista (tile de cuenta)
                    control_clicked = False
                    for ctype in ('ListItem', 'Button', 'DataItem'):
                        try:
                            ctrl = dlg.child_window(control_type=ctype, found_index=0)
                            ctrl.click_input()
                            logging.info(f"[LOGIN][C] Click en control_type='{ctype}'.")
                            control_clicked = True
                            break
                        except Exception as ce:
                            logging.warning(f"[LOGIN][C] No control_type='{ctype}': {ce}")
                    if not control_clicked:
                        # Último intento: listar todos los controles disponibles
                        try:
                            desc = dlg.print_control_identifiers()
                            logging.info(f"[LOGIN][C] Árbol de controles: {desc}")
                        except Exception:
                            pass
                    time.sleep(1.5)
                    hwnd_check, _ = self._find_window_hwnd(title_patterns)
                    if hwnd_check is None:
                        logging.info("[LOGIN][C] ✅ Ventana cerrada con pywinauto UIA.")
                        self.login_window_handled = True
                        clicked = True
                except Exception as e:
                    logging.warning(f"[LOGIN][C] pywinauto UIA error: {e}")

            # ── Estrategia D: SendInput mouse (DPI-aware) ─────────────────────
            if not clicked:
                click_x = wx + ww // 2
                logging.info("[LOGIN][D] Iniciando clicks con SendInput mouse...")
                for y_off in [245, 260, 230, 275, 210, 195, 300, 180, 320]:
                    click_y = wy + y_off
                    if not (wy < click_y < wy2):
                        continue
                    ax = int(click_x * 65535 / SM_W)
                    ay = int(click_y * 65535 / SM_H)
                    logging.info(f"[LOGIN][D] SendInput ({click_x},{click_y}) abs=({ax},{ay}) y_off={y_off}")
                    try:
                        send_mouse_click(ax, ay)
                        time.sleep(1.5)
                        hwnd_check, _ = self._find_window_hwnd(title_patterns)
                        if hwnd_check is None:
                            logging.info(f"[LOGIN][D] ✅ Cerrada. y_off={y_off} funcionó.")
                            self.login_window_handled = True
                            clicked = True
                            break
                        logging.info(f"[LOGIN][D] Ventana sigue visible (y_off={y_off}).")
                    except Exception as e:
                        logging.warning(f"[LOGIN][D] y_off={y_off}: {e}")

            # ── Estrategia E: PostMessage a hijos Chrome_WidgetWin_1 ──────────
            if not clicked:
                logging.info("[LOGIN][E] Probando PostMessage a hijos Chrome_WidgetWin_1...")
                try:
                    children = []
                    def child_cb(ch, _):
                        try:
                            if win32gui.GetClassName(ch) == "Chrome_WidgetWin_1":
                                children.append(ch)
                        except Exception:
                            pass
                    win32gui.EnumChildWindows(hwnd, child_cb, None)
                    logging.info(f"[LOGIN][E] Hijos Chrome_WidgetWin_1 encontrados: {children}")
                    for ch in children:
                        for y_off in [245, 260, 230]:
                            lp = (y_off << 16) | (ww // 2 & 0xFFFF)
                            win32api.PostMessage(ch, win32con.WM_LBUTTONDOWN, win32con.MK_LBUTTON, lp)
                            time.sleep(0.1)
                            win32api.PostMessage(ch, win32con.WM_LBUTTONUP, 0, lp)
                            time.sleep(1.0)
                            hwnd_check, _ = self._find_window_hwnd(title_patterns)
                            if hwnd_check is None:
                                logging.info(f"[LOGIN][E] ✅ Cerrada. ch={ch} y_off={y_off}.")
                                self.login_window_handled = True
                                clicked = True
                                break
                        if clicked:
                            break
                except Exception as e:
                    logging.warning(f"[LOGIN][E] Error: {e}")

            if not clicked:
                logging.warning("[LOGIN] Ninguna estrategia funcionó. Reintentando en 3s...")
                time.sleep(3)
                waited += 3

        if not self.login_window_handled:
            logging.warning(f"[LOGIN] No manejado tras {max_wait}s.")


    def _open_and_process_excel(self):
        excel = win32com.client.Dispatch("Excel.Application")
        try:
            logging.info("[EXCEL] Abriendo Excel y archivo: %s", self.file_path)
            excel.Visible = True

            # El diálogo 'Pick an account' puede aparecer durante Workbooks.Open,
            # durante la asignación del slicer (VisibleSlicerItemsList) o durante
            # connection.Refresh() — arrancamos el hilo antes de cualquier operación.
            logging.info("[EXCEL] Iniciando hilo de monitoreo de login window...")
            self.login_window_handled = False
            threading.Thread(target=self.handle_microsoft_login_window, daemon=True).start()

            workbook = excel.Workbooks.Open(self.file_path)

            current_date = datetime.datetime.now()
            current_month_name = current_date.strftime("%Y%m")
            last_month_date = current_date - datetime.timedelta(days=10)
            last_month_name = last_month_date.strftime("%Y%m")

            logging.info(
                "[EXCEL] Obteniendo slicer principal 'SegmentaciónDeDatos_Período'"
            )
            slicer_cache = workbook.SlicerCaches("SegmentaciónDeDatos_Período")
            logging.info(
                f"[EXCEL] VisibleSlicerItemsList actual: {slicer_cache.VisibleSlicerItemsList}"
            )

            current_month_value = f"[Calendario].[Período].&[{current_month_name}]"

            if slicer_cache.VisibleSlicerItemsList == [current_month_value]:
                logging.info(
                    "[EXCEL] El slicer principal ya está seleccionado en el valor actual."
                )
                for connection in workbook.Connections:
                    logging.info(f"[EXCEL] Refrescando conexión: {connection.Name}")
                    before_refresh = getattr(connection, 'RefreshDate', None)
                    connection.Refresh()
                    self.wait_until_excel_ready(excel, timeout=180)
                    after_refresh = getattr(connection, 'RefreshDate', None)
                    logging.info(f"[EXCEL] Estado conexión: {connection.Name} | Antes: {before_refresh} | Después: {after_refresh}")
                    # Loguear errores OLEDB si existen
                    try:
                        oledb_errors = getattr(connection, 'OLEDBErrors', None)
                        if oledb_errors and hasattr(oledb_errors, 'Count') and oledb_errors.Count > 0:
                            for i in range(1, oledb_errors.Count+1):
                                err = oledb_errors.Item(i)
                                logging.error(f"[EXCEL][OLEDBError] Conexión: {connection.Name} | Error: {err.Description}")
                    except Exception as e:
                        logging.error(f"[EXCEL][OLEDBError] No se pudo obtener errores OLEDB para {connection.Name}: {e}")
            else:
                logging.info(
                    "[EXCEL] Seleccionando nuevo valor en el slicer principal..."
                )
                # slicer_cache.ClearAllFilters()
                slicer_cache.VisibleSlicerItemsList = [current_month_value]
                self.wait_until_excel_ready(excel, timeout=90)

                for connection in workbook.Connections:
                    logging.info(f"[EXCEL] Refrescando conexión: {connection.Name}")
                    before_refresh = getattr(connection, 'RefreshDate', None)
                    connection.Refresh()
                    self.wait_until_excel_ready(excel, timeout=180)
                    after_refresh = getattr(connection, 'RefreshDate', None)
                    logging.info(f"[EXCEL] Estado conexión: {connection.Name} | Antes: {before_refresh} | Después: {after_refresh}")
                    try:
                        oledb_errors = getattr(connection, 'OLEDBErrors', None)
                        if oledb_errors and hasattr(oledb_errors, 'Count') and oledb_errors.Count > 0:
                            for i in range(1, oledb_errors.Count+1):
                                err = oledb_errors.Item(i)
                                logging.error(f"[EXCEL][OLEDBError] Conexión: {connection.Name} | Error: {err.Description}")
                    except Exception as e:
                        logging.error(f"[EXCEL][OLEDBError] No se pudo obtener errores OLEDB para {connection.Name}: {e}")

            period_values = []
            for i in range(3):
                month_date = current_date - relativedelta(months=i)
                month_name = month_date.strftime("%Y%m")
                period_values.append(f"[Calendario].[Período].&[{month_name}]")

            logging.info(
                "[EXCEL] Obteniendo slicer clientes 'SegmentaciónDeDatos_Período1'"
            )
            slicer_cache_clientes = workbook.SlicerCaches(
                "SegmentaciónDeDatos_Período1"
            )
            logging.info(
                f"[EXCEL] VisibleSlicerItemsList clientes actual: {slicer_cache_clientes.VisibleSlicerItemsList}"
            )

            if set(slicer_cache_clientes.VisibleSlicerItemsList) == set(period_values):
                logging.info(
                    "[EXCEL] El slicer clientes ya está seleccionado en los valores correctos."
                )
            else:
                logging.info(
                    "[EXCEL] Seleccionando nuevos valores en el slicer clientes..."
                )
                # slicer_cache_clientes.ClearAllFilters()
                slicer_cache_clientes.VisibleSlicerItemsList = period_values
                self.wait_until_excel_ready(excel, timeout=90)
                for connection in workbook.Connections:
                    logging.info(f"[EXCEL] Refrescando conexión: {connection.Name}")
                    connection.Refresh()
                    self.wait_until_excel_ready(excel, timeout=90)

            logging.info("[EXCEL] Esperando 10s para sincronización de OneDrive...")
            time.sleep(10)

            logging.info(f"[EXCEL] Guardando copia local en: {self.local_copy_path}")
            self.wait_until_excel_ready(excel, timeout=60)
            workbook.SaveCopyAs(self.local_copy_path)
            workbook.Close(SaveChanges=False)
            logging.info(f"[EXCEL] Abriendo copia local: {self.local_copy_path}")
            copy_workbook = excel.Workbooks.Open(self.local_copy_path)

            for connection in copy_workbook.Connections:
                logging.info(f"[EXCEL] Eliminando conexión: {connection.Name}")
                connection.Delete()

            logging.info(
                "[EXCEL] Esperando 2s antes de guardar y cerrar la copia local..."
            )
            time.sleep(2)

            copy_workbook.Save()
            copy_workbook.Close()

            excel.Quit()
            logging.info("[EXCEL] Termina proceso Excel")
            time.sleep(2)

            self.send_email()

        except Exception as e:
            logging.error(f"[EXCEL] Error during Excel processing: {e}")
        finally:
            try:
                workbook.Close(SaveChanges=False)
            except Exception:
                pass
            try:
                excel.Quit()
            except Exception:
                pass
            logging.info("[EXCEL] Excel process completed.")

    def send_email(self):
        logging.info("Inicia envío de correos")
        # Indica que vas a usar las variables globales
        sql = text("SELECT * FROM powerbi_adm.conf_tipo WHERE nbTipo = '11';")
        # print(sql)
        df = self.config_basic.execute_sql_query(sql)
        # print(df)
        if not df.empty:
            # Corrige la asignación aquí
            self.config["nmUsrCorreo"] = df["nmUsr"].iloc[0]
            self.config["txPassCorreo"] = df["txPass"].iloc[0]
        else:
            # Considera si necesitas manejar el caso de un DataFrame vacío de manera diferente
            print("No se encontraron configuraciones de Correo.")

        host = "mail.amovil.com.co"
        port = 587
        username = self.config["nmUsrCorreo"]
        password = self.config["txPassCorreo"]

        from_addr = "amovildesk@amovil.com.co"
        to_addr = [
            "lider.proyectos@amovil.co",
        ]
        bcc_addr = [
            "cesar.trujillo@amovil.co",
        ]
        with open("difusion.txt", "r") as file:
            cc_addr = [line.strip() for line in file]

        msg = MIMEMultipart()
        msg["From"] = from_addr
        msg["To"] = COMMASPACE.join(to_addr)
        msg["Cc"] = COMMASPACE.join(cc_addr)
        msg["Bcc"] = COMMASPACE.join(bcc_addr)
        msg["Subject"] = "Analitica Compi tienda"

        # logo_path = "logouau.png"

        # with open(logo_path, "rb") as logo_file:
        #     logo_data = logo_file.read()

        # logo_image = MIMEImage(logo_data)
        # logo_image.add_header("Content-ID", "<logo>")
        # msg.attach(logo_image)

        html_message = f"""
        <html>
            <head>
            <style>
            body {{ font-family: Arial; }}
            p {{ font-size: 12px; }}
            </style>
            </head>
            <body>
                <h3>Cordial Saludo,</h3>
                <p>Adjunto encontrará el seguimiento acumulado de Compi tienda, cuya información corresponde al mes actual y hace referencia a las cifras consolidadas en la aplicación.</p>
                <p>Mensaje generado de manera automática, por favor no responder.</p>
                <br>
                <img src="https://drive.google.com/uc?export=view&id=1nCnHpLIepkZ37MJPuAxcOMyeq0Bg2Iny" alt="Logo de la empresa">
            </body>
        </html>
        """

        msg.attach(MIMEText(html_message, "html"))

        # filename = self.local_copy_path.split("\\")[-1]
        # self.local_copy_path
        # filename = os.path.basename(self.local_copy_path)
        # Adjuntar el archivo
        with open(self.local_copy_path, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())

        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition", f"attachment; filename= {self.local_copy_path}"
        )

        msg.attach(part)

        # Intentar enviar el correo hasta tres veces en caso de error
        max_retries = 3
        exceptions = []  # Lista para almacenar excepciones
        sent_successfully = (
            False  # Variable para rastrear si el correo se envió con éxito
        )

        for _ in range(max_retries):
            try:
                with smtplib.SMTP(host, port) as server:
                    server.starttls()
                    server.login(username, password)
                    server.sendmail(from_addr, to_addr + cc_addr, msg.as_string())
                    logging.info("Proceso de envío de correo completado")
                    sent_successfully = True  # El correo se envió con éxito
                # Si el correo se envió con éxito, salir del bucle
                break
            except Exception as e:
                exceptions.append(e)  # Guardar la excepción en la lista
                logging.error(f"Error al enviar el correo: {e}")
                error_message = "Error en el proceso de envío de correo: " + str(e)
                logging.error(error_message)
                self.send_email_notification(error_message)

        if sent_successfully:
            # Elimina el archivo una vez que el correo se ha enviado con éxito
            try:
                os.remove(self.local_copy_path)
                print("Archivo eliminado exitosamente.")
                logging.info("Archivo eliminado exitosamente.")
            except Exception as e:
                logging.error(f"Error al eliminar el archivo: {e}")
                print(f"Error al eliminar el archivo: {e}")

    def send_email_notification(self, error_message):
        logging.info("Inicia envío de correos")
        # Indica que vas a usar las variables globales

        host = "smtp.gmail.com"
        port = 587
        username = "torredecontrolamovil@gmail.com"
        password = "dldaqtceiesyybje"
        from_addr = "torredecontrolamovil@gmail.com"
        to_addr = [
            "cesar.trujillo@amovil.co",
        ]
        with open("difusionerror.txt", "r") as file:
            cc_addr = [line.strip() for line in file]

        msg = MIMEMultipart()
        msg["From"] = from_addr
        msg["To"] = COMMASPACE.join(to_addr)
        msg["Cc"] = COMMASPACE.join(cc_addr)
        msg["Subject"] = f"Error Compi Tienda"

        html_message = f"""
        <html>
            <head>
            <style>
            body {{ font-family: Arial; }}
            p {{ font-size: 12px; }}
            </style>
            </head>
            <body>
                <h3>Verifica el log,</h3>
                <p>Adjunto encontrará el log de ejecución observe detalladamente cual proceso no se completo</p>
                <br>
            </body>
        </html>
        """

        msg.attach(MIMEText(html_message, "html"))

        filename = "log.txt"
        # Adjuntar el archivo
        with open(filename, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())

        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename= {filename}")

        msg.attach(part)

        # Intentar enviar el correo hasta tres veces en caso de error
        max_retries = 3

        for _ in range(max_retries):
            try:
                with smtplib.SMTP(host, port) as server:
                    server.starttls()
                    server.login(username, password)
                    server.sendmail(from_addr, to_addr + cc_addr, msg.as_string())
                # Si el correo se envió con éxito, salir del bucle
                break
            except Exception as e:
                logging.error(f"Error al enviar el correo: {e}")
                # Puedes agregar un retraso aquí si lo deseas antes de reintentar

        logging.info("Proceso de envío de correo completado")
