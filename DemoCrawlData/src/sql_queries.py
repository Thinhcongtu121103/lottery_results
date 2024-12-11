# sql_queries.py
# Data loader to Staging
LS_DELETE_QUERY = """
    DELETE FROM lottery_results
"""

LS_INSERT_QUERY = """
    INSERT INTO lottery_results (ngay_quay_xo_so, gio_xo_so, mien, tinh, g8, g7, g6, g5, g4, g3, g2, g1, db, draw_date, draw_time)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# DataTransform
CREATE_DIM_REGION = """
CREATE TABLE IF NOT EXISTS dim_region (
    region_id INT PRIMARY KEY AUTO_INCREMENT,
    region_name VARCHAR(50) UNIQUE
)
"""

CREATE_DIM_PROVINCE = """
CREATE TABLE IF NOT EXISTS dim_province (
    province_id INT PRIMARY KEY AUTO_INCREMENT,
    province_name VARCHAR(100) UNIQUE,
    region_id INT,
    FOREIGN KEY (region_id) REFERENCES dim_region(region_id)
)
"""

CREATE_DIM_DATE = """
CREATE TABLE IF NOT EXISTS dim_date (
    date_id INT PRIMARY KEY AUTO_INCREMENT,
    draw_date DATE UNIQUE
)
"""

CREATE_DIM_TIME = """
CREATE TABLE IF NOT EXISTS dim_time (
    time_id INT PRIMARY KEY AUTO_INCREMENT,
    time_period VARCHAR(20) UNIQUE
)
"""

CREATE_DIM_DATE_LOTTERY = """
CREATE TABLE IF NOT EXISTS dim_date_lottery (
    date_lottery_id INT PRIMARY KEY AUTO_INCREMENT,
    date_lottery VARCHAR(20) UNIQUE
)
"""

CREATE_DIM_TIME_LOTTERY = """
CREATE TABLE IF NOT EXISTS dim_time_lottery (
    time_lottery_id INT PRIMARY KEY AUTO_INCREMENT,
    time_lottery VARCHAR(20) UNIQUE
)
"""

INSERT_DIM_REGION = "INSERT IGNORE INTO dim_region (region_name) VALUES (%s)"
INSERT_DIM_PROVINCE = "INSERT IGNORE INTO dim_province (province_name) VALUES (%s)"
INSERT_DIM_DATE = "INSERT IGNORE INTO dim_date (draw_date) VALUES (%s)"
INSERT_DIM_TIME = "INSERT IGNORE INTO dim_time (draw_time) VALUES (%s)"
INSERT_DIM_DATE_LOTTERY = "INSERT IGNORE INTO dim_date_lottery (date_lottery) VALUES (%s)"
INSERT_DIM_TIME_LOTTERY = "INSERT IGNORE INTO dim_time_lottery (time_lottery) VALUES (%s)"
