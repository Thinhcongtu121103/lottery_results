def check_dw_exists(db_connector, dw_name="dw_lottery"):
    """
    Kiểm tra xem database DW đã tồn tại hay chưa.
    """
    cursor = db_connector.connection.cursor()
    cursor.execute(f"SHOW DATABASES LIKE '{dw_name}';")
    result = cursor.fetchone()
    cursor.close()
    return result is not None

def create_dw_schema(db_connector):
    cursor = db_connector.connection.cursor()

    # Sử dụng database `dw_lottery` nếu đã tồn tại, nếu không thì tạo mới
    cursor.execute("CREATE DATABASE IF NOT EXISTS dw_lottery CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
    print("Database `dw_lottery` đã được tạo hoặc đã tồn tại.")
    cursor.execute("USE dw_lottery;")

    # Tạo bảng `dim_region`
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_region (
            region_id INT AUTO_INCREMENT PRIMARY KEY,
            region_name VARCHAR(255) UNIQUE NOT NULL
        );
    """)

    # Tạo bảng `dim_province`
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_province (
            province_id INT AUTO_INCREMENT PRIMARY KEY,
            province_name VARCHAR(255) UNIQUE NOT NULL,
            region_id INT,
            FOREIGN KEY (region_id) REFERENCES dim_region(region_id)
        );
    """)

    # Tạo bảng `fact_lottery_results`
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_lottery_results (
            result_id INT AUTO_INCREMENT PRIMARY KEY,
            draw_date DATE NOT NULL,
            draw_time TIME NOT NULL,
            region_id INT,
            province_id INT,
            g8 VARCHAR(255),
            g7 VARCHAR(255),
            g6 VARCHAR(255),
            g5 VARCHAR(255),
            g4 VARCHAR(255),
            g3 VARCHAR(255),
            g2 VARCHAR(255),
            g1 VARCHAR(255),
            db VARCHAR(255),
            FOREIGN KEY (region_id) REFERENCES dim_region(region_id),
            FOREIGN KEY (province_id) REFERENCES dim_province(province_id)
        );
    """)

    print("Schema cho Data Warehouse đã được thiết lập thành công.")
    cursor.close()
