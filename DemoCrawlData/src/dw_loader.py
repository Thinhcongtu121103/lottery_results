class DWLoader:
    def __init__(self, db_connector):
        self.db_connector = db_connector
        self.connection = db_connector.connection

    def ensure_table_exists(self, schema_name, table_name, create_statement):
        """
        Kiểm tra nếu bảng không tồn tại trong schema cụ thể thì tạo bảng.
        """
        cursor = self.connection.cursor()
        try:
            # Kiểm tra bảng có tồn tại không trong schema cụ thể
            cursor.execute(f"""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'
            """)
            result = cursor.fetchone()
            if not result:
                print(f"Bảng {table_name} chưa tồn tại trong schema {schema_name}. Đang tạo bảng...")
                cursor.execute(create_statement)
                print(f"Bảng {table_name} đã được tạo thành công.")
            else:
                print(f"Bảng {table_name} đã tồn tại trong schema {schema_name}.")
        except Exception as e:
            print(f"Lỗi khi kiểm tra hoặc tạo bảng {table_name} trong schema {schema_name}: {e}")
            raise
        finally:
            cursor.close()

    def load_to_dw(self):
        # Các lệnh tạo bảng nếu chưa tồn tại
        create_dim_region = """
            CREATE TABLE IF NOT EXISTS dw_lottery.dim_region (
                region_id INT PRIMARY KEY,
                region_name VARCHAR(255)
            )
        """
        create_dim_province = """
            CREATE TABLE IF NOT EXISTS dw_lottery.dim_province (
                province_id INT PRIMARY KEY,
                province_name VARCHAR(255),
                region_id INT,
                FOREIGN KEY (region_id) REFERENCES dw_lottery.dim_region(region_id)
            )
        """
        create_dim_date = """
            CREATE TABLE IF NOT EXISTS dw_lottery.dim_date (
                date_id INT PRIMARY KEY,
                draw_date DATE
            )
        """
        create_dim_date_lottery = """
            CREATE TABLE IF NOT EXISTS dw_lottery.dim_date_lottery (
                date_lottery_id INT PRIMARY KEY,
                date_lottery VARCHAR(255)
            )
        """

        create_dim_time_lottery = """
                    CREATE TABLE IF NOT EXISTS dw_lottery.dim_time_lottery (
                        time_lottery_id INT PRIMARY KEY,
                        time_lottery VARCHAR(255)
                    )
                """
        create_dim_time = """
            CREATE TABLE IF NOT EXISTS dw_lottery.dim_time (
                time_id INT PRIMARY KEY,
                draw_time TIME
            )
        """
        create_fact_lottery_results = """
            CREATE TABLE IF NOT EXISTS dw_lottery.fact_lottery_results (
                draw_date DATE,
                draw_time TIME,
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
                PRIMARY KEY (draw_date, province_id),
                FOREIGN KEY (region_id) REFERENCES dw_lottery.dim_region(region_id),
                FOREIGN KEY (province_id) REFERENCES dw_lottery.dim_province(province_id)
            )
        """

        # Kiểm tra và tạo bảng trước khi chèn dữ liệu
        # Kiểm tra và tạo bảng trước khi chèn dữ liệu
        self.ensure_table_exists("dw_lottery", "dim_region", create_dim_region)
        self.ensure_table_exists("dw_lottery", "dim_province", create_dim_province)
        self.ensure_table_exists("dw_lottery", "dim_date", create_dim_date)
        self.ensure_table_exists("dw_lottery", "dim_date_lottery", create_dim_date_lottery)
        self.ensure_table_exists("dw_lottery", "dim_time_lottery", create_dim_time_lottery)
        self.ensure_table_exists("dw_lottery", "dim_time", create_dim_time)
        self.ensure_table_exists("dw_lottery", "fact_lottery_results", create_fact_lottery_results)

        # Bắt đầu chèn dữ liệu
        cursor = self.connection.cursor()
        try:
            # Chuyển dữ liệu vào bảng dim_region
            print("Đang chuyển dữ liệu vào bảng dim_region...")
            cursor.execute("""
                INSERT IGNORE INTO dw_lottery.dim_region (region_id, region_name)
                SELECT DISTINCT region_id, region_name
                FROM staging.dim_region
            """)

            # Chuyển dữ liệu vào bảng dim_province
            print("Đang chuyển dữ liệu vào bảng dim_province...")
            cursor.execute("""
                INSERT IGNORE INTO dw_lottery.dim_province (province_id, province_name, region_id)
                SELECT DISTINCT province_id, province_name, region_id
                FROM staging.dim_province
            """)

            # Chuyển dữ liệu vào bảng dim_date
            print("Đang chuyển dữ liệu vào bảng dim_date...")
            cursor.execute("""
                INSERT IGNORE INTO dw_lottery.dim_date (date_id, draw_date)
                SELECT DISTINCT date_id, draw_date
                FROM staging.dim_date
            """)

            # Chuyển dữ liệu vào bảng dim_date_lottery
            print("Đang chuyển dữ liệu vào bảng dim_date_lottery...")
            cursor.execute("""
                INSERT IGNORE INTO dw_lottery.dim_date_lottery (date_lottery_id, date_lottery)
                SELECT DISTINCT date_lottery_id, date_lottery
                FROM staging.dim_date_lottery
                        """)

            # Chuyển dữ liệu vào bảng dim_time_lottery
            print("Đang chuyển dữ liệu vào bảng dim_time_lottery...")
            cursor.execute("""
                            INSERT IGNORE INTO dw_lottery.dim_time_lottery (time_lottery_id, time_lottery)
                            SELECT DISTINCT time_lottery_id, time_lottery
                            FROM staging.dim_time_lottery
                                    """)

            # Chuyển dữ liệu vào bảng dim_time
            print("Đang chuyển dữ liệu vào bảng dim_time...")
            cursor.execute("""
                INSERT IGNORE INTO dw_lottery.dim_time (time_id, draw_time)
                SELECT DISTINCT time_id, draw_time
                FROM staging.dim_time
            """)

            # Chuyển dữ liệu vào bảng fact_lottery_results
            print("Đang chuyển dữ liệu vào bảng fact_lottery_results...")
            cursor.execute("""
                INSERT INTO dw_lottery.fact_lottery_results (date_lottery_id, time_lottery_id, date_id, time_id, region_id, province_id, g8, g7, g6, g5, g4, g3, g2, g1, db)
                SELECT date_lottery_id, time_lottery_id, date_id, time_id, region_id, province_id, g8, g7, g6, g5, g4, g3, g2, g1, db
                FROM staging.fact_lottery_results
            """)

            # Commit các thay đổi
            self.connection.commit()
            print("Dữ liệu đã được chuyển thành công vào Data Warehouse.")

        except Exception as e:
            self.connection.rollback()
            print("Lỗi khi chuyển dữ liệu vào DW:", e)
            raise  # Ném lại lỗi để main() xử lý

        finally:
            cursor.close()
