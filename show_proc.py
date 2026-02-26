from scripts.config import ConfigBasic
from scripts.conexion import Conexion


def main():
    import sys

    proc_name = sys.argv[1] if len(sys.argv) > 1 else 'sp_infoventas_full_maintenance'

    config = ConfigBasic('distrijass').config
    engine = Conexion.ConexionMariadb3(
        config['nmUsrIn'],
        config['txPassIn'],
        config['hostServerIn'],
        int(config['portServerIn']),
        config['dbBi'],
    )
    conn = engine.raw_connection()
    try:
        cursor = conn.cursor()
        try:
            cursor.execute(f"SHOW CREATE PROCEDURE {proc_name}")
            row = cursor.fetchone()
            if row and len(row) >= 3:
                print(row[2])
        finally:
            cursor.close()
    finally:
        conn.close()


if __name__ == '__main__':
    main()
