import pandas as pd
from sql_producer import SQLProducer

class DataLoader:
    def __init__(self, db_connector, csv_path, sql_folder):
        """
        Khởi tạo DataLoader với đối tượng kết nối database, đường dẫn đến file CSV
        và thư mục chứa các file SQL.

        Args:
            db_connector: Đối tượng DatabaseConnector để kết nối với cơ sở dữ liệu.
            csv_path (str): Đường dẫn đến file CSV chứa dữ liệu cần load.
            sql_folder (str): Đường dẫn đến thư mục chứa các file SQL.
        """
        self.db_connector = db_connector
        self.csv_path = csv_path
        self.sql_producer = SQLProducer(sql_folder)
        self.sql_producer.load_queries("queries.sql")  # Load file SQL
    # 3.2. Phương thức load to staging
    def load_to_staging(self):
        """
        Đọc dữ liệu từ file CSV và load vào bảng staging trong cơ sở dữ liệu.
        """
        # Đọc dữ liệu từ file CSV
        data = pd.read_csv(self.csv_path)

        # Kết nối đến database
        conn = self.db_connector.connection
        cursor = conn.cursor()

        # Lấy câu lệnh SQL
        insert_query = self.sql_producer.get_query("insert_lottery_results")

        # Duyệt từng dòng và chèn vào bảng staging
        record_count = 0
        for _, row in data.iterrows():
            row = row.fillna('')  # Thay thế NaN bằng chuỗi rỗng hoặc None nếu cần
            g8_value = row['G8']
            if isinstance(g8_value, float):
                g8_value = str(int(g8_value))  # Loại bỏ .0 và chuyển thành chuỗi
            g8_value = g8_value.zfill(2)  # Thêm số 0 phía trước nếu cần

            # Dữ liệu cho câu lệnh SQL
            data_dict = {
                "ngay_quay_xo_so": row['Ngày quay xổ số'],
                "gio_xo_so": row['Giờ xổ số'],
                "mien": row['Miền'],
                "tinh": row['Tỉnh'],
                "g8": g8_value,
                "g7": row['G7'],
                "g6": row['G6'],
                "g5": row['G5'],
                "g4": row['G4'],
                "g3": row['G3'],
                "g2": row['G2'],
                "g1": row['G1'],
                "db": row['ĐB'],
                "draw_date": row['draw_date'],
                "draw_time": row['draw_time'],
            }

            # Thực hiện câu lệnh SQL
            try:
                cursor.execute(insert_query, data_dict)
                record_count += 1
            except Exception as e:
                print(f"Lỗi khi chèn dòng: {row} - {e}")

        # Commit và đóng kết nối
        conn.commit()
        cursor.close()
        print(f"Dữ liệu đã được load vào bảng staging: {record_count} dòng.")
        return record_count
