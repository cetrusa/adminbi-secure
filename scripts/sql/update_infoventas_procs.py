"""Actualiza los procedimientos almacenados de infoventas para optimizar la
sincronización por año.
"""

from scripts.config import ConfigBasic
from scripts.conexion import Conexion


STATEMENTS = [
    "DROP PROCEDURE IF EXISTS sp_infoventas_sync_fact_dev_year",
    """
    CREATE PROCEDURE sp_infoventas_sync_fact_dev_year(IN p_year INT)
    BEGIN
      DECLARE v_tbl VARCHAR(64);
      DECLARE v_tbl_fact VARCHAR(64);
      DECLARE v_tbl_dev VARCHAR(64);

      IF p_year IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'p_year no puede ser NULL';
      END IF;

      SET v_tbl = CONCAT('infoventas_', p_year);
      SET v_tbl_fact = CONCAT(v_tbl, '_fact');
      SET v_tbl_dev  = CONCAT(v_tbl, '_dev');

      CALL sp_infoventas_create_year_table(p_year);

      SET @createFact = CONCAT('CREATE TABLE IF NOT EXISTS `', v_tbl_fact, '` LIKE `', v_tbl, '`;');
      PREPARE stmt FROM @createFact; EXECUTE stmt; DEALLOCATE PREPARE stmt;

      SET @createDev = CONCAT('CREATE TABLE IF NOT EXISTS `', v_tbl_dev, '` LIKE `', v_tbl, '`;');
      PREPARE stmt FROM @createDev; EXECUTE stmt; DEALLOCATE PREPARE stmt;

      START TRANSACTION;

      SET @insFact = CONCAT(
        'INSERT INTO `', v_tbl_fact, '` SELECT * FROM `', v_tbl, '` WHERE CAST(`Tipo` AS CHAR) = ''0'' ',
        'ON DUPLICATE KEY UPDATE `Cantidad`=VALUES(`Cantidad`), `Vta neta`=VALUES(`Vta neta`), `Costo`=VALUES(`Costo`);'
      );
      PREPARE stmt FROM @insFact; EXECUTE stmt; DEALLOCATE PREPARE stmt;

      SET @insDev = CONCAT(
        'INSERT INTO `', v_tbl_dev, '` SELECT * FROM `', v_tbl, '` WHERE CAST(`Tipo` AS CHAR) = ''1'' ',
        'ON DUPLICATE KEY UPDATE `Cantidad`=VALUES(`Cantidad`), `Vta neta`=VALUES(`Vta neta`), `Costo`=VALUES(`Costo`);'
      );
      PREPARE stmt FROM @insDev; EXECUTE stmt; DEALLOCATE PREPARE stmt;

      SET @del = CONCAT('DELETE FROM `', v_tbl, '` WHERE CAST(`Tipo` AS CHAR) IN (''0'',''1'');');
      PREPARE stmt FROM @del; EXECUTE stmt; DEALLOCATE PREPARE stmt;

      COMMIT;
    END
    """,
    "DROP PROCEDURE IF EXISTS sp_infoventas_migrate_pending",
    """
    CREATE PROCEDURE sp_infoventas_migrate_pending()
    BEGIN
      DECLARE done INT DEFAULT 0;
      DECLARE v_year INT;
      DECLARE v_table VARCHAR(64);

      DECLARE c CURSOR FOR
        SELECT DISTINCT YEAR(`Fecha`) AS y
        FROM `infoventas`
        WHERE `Fecha` IS NOT NULL
        ORDER BY y;

      DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

      CREATE TEMPORARY TABLE IF NOT EXISTS tmp_infoventas_years (
        `year` INT PRIMARY KEY
      );

      OPEN c;
      read_loop: LOOP
        FETCH c INTO v_year;
        IF done = 1 THEN LEAVE read_loop; END IF;

        SET v_table = CONCAT('infoventas_', v_year);

        SET @sql = CONCAT('CREATE TABLE IF NOT EXISTS `', v_table, '` LIKE `infoventas`;');
        PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

        START TRANSACTION;

        CALL sp_infoventas_migrate_year(v_year);

        SET @sql_del = CONCAT('DELETE FROM infoventas WHERE YEAR(`Fecha`) = ', v_year, ';');
        PREPARE stmt FROM @sql_del; EXECUTE stmt; DEALLOCATE PREPARE stmt;

        INSERT IGNORE INTO tmp_infoventas_years(`year`) VALUES (v_year);

        COMMIT;
      END LOOP;
      CLOSE c;
    END
    """,
    "DROP PROCEDURE IF EXISTS sp_infoventas_full_maintenance",
    """
    CREATE PROCEDURE sp_infoventas_full_maintenance()
    BEGIN
      DECLARE v_year INT DEFAULT YEAR(CURDATE());
      DECLARE v_processed_year INT;
      DECLARE v_years_remaining INT;

      DROP TEMPORARY TABLE IF EXISTS tmp_infoventas_years;
      CREATE TEMPORARY TABLE tmp_infoventas_years (
        `year` INT PRIMARY KEY
      );

      CALL sp_infoventas_ensure_current_next_year();
      CALL sp_infoventas_migrate_pending();
      CALL sp_infoventas_rebuild_view();

      IF NOT EXISTS (SELECT 1 FROM tmp_infoventas_years) THEN
        INSERT IGNORE INTO tmp_infoventas_years(`year`) VALUES (v_year);
      END IF;

      SET v_years_remaining = (SELECT COUNT(*) FROM tmp_infoventas_years);

      WHILE v_years_remaining > 0 DO
        SELECT `year` INTO v_processed_year
        FROM tmp_infoventas_years
        ORDER BY `year` ASC
        LIMIT 1;

        IF v_processed_year IS NOT NULL THEN
          CALL sp_infoventas_sync_fact_dev_year(v_processed_year);
          DELETE FROM tmp_infoventas_years WHERE `year` = v_processed_year;
        END IF;

        SET v_years_remaining = (SELECT COUNT(*) FROM tmp_infoventas_years);
      END WHILE;

      DELETE FROM infoventas WHERE YEAR(Fecha) <= v_year;

      DROP TEMPORARY TABLE IF EXISTS tmp_infoventas_years;
    END
    """,
]


def run():
    config = ConfigBasic('distrijass').config
    engine = Conexion.ConexionMariadb3(
        config['nmUsrIn'],
        config['txPassIn'],
        config['hostServerIn'],
        int(config['portServerIn']),
        config['dbBi'],
    )

    for sql in STATEMENTS:
        conn = engine.raw_connection()
        try:
            cursor = conn.cursor()
            try:
                cursor.execute(sql)
                conn.commit()
            finally:
                cursor.close()
        finally:
            conn.close()


if __name__ == '__main__':
    run()
