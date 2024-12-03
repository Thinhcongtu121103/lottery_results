class DWLoader:
    def __init__(self, db_connector):
        self.db_connector = db_connector
        self.connection = db_connector.connection

    def load_to_dw(self):
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

            # Chuyển dữ liệu vào bảng fact_lottery_results
            print("Đang chuyển dữ liệu vào bảng fact_lottery_results...")
            cursor.execute("""
                INSERT INTO dw_lottery.fact_lottery_results (draw_date, draw_time, region_id, province_id, g8, g7, g6, g5, g4, g3, g2, g1, db)
                SELECT draw_date, draw_time, region_id, province_id, g8, g7, g6, g5, g4, g3, g2, g1, db
                FROM staging.fact_lottery_results
            """)

            # Commit các thay đổi
            self.connection.commit()
            print("Dữ liệu đã được chuyển thành công vào Data Warehouse.")

        except Exception as e:
            self.connection.rollback()
            print("Lỗi khi chuyển dữ liệu vào DW:", e)

        finally:
            cursor.close()
