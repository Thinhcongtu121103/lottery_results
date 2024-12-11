import pandas as pd

from src import sql_queries


class DataLoader:
    def __init__(self, db_connector, csv_path):
        """
        Khởi tạo DataLoader với đối tượng kết nối database và đường dẫn đến file CSV.

        Args:
            db_connector: Đối tượng DatabaseConnector để kết nối với cơ sở dữ liệu.
            csv_path (str): Đường dẫn đến file CSV chứa dữ liệu cần load.
        """
        self.db_connector = db_connector  # Đối tượng kết nối cơ sở dữ liệu
        self.csv_path = csv_path  # Đường dẫn file CSV

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
        cursor.execute(sql_queries.LS_DELETE_QUERY)

        # Duyệt từng dòng và gọi stored procedure để chèn vào bảng staging
        record_count = 0
        for _, row in data.iterrows():
            # Thay thế NaN bằng chuỗi rỗng hoặc giá trị mặc định trước khi chèn
            row = row.fillna('')  # Hoặc thay bằng None nếu bạn muốn NULL trong SQL
            g8_value = row['G8']
            if isinstance(g8_value, float):
                g8_value = str(int(g8_value))  # Loại bỏ .0 và chuyển thành chuỗi
            # Nếu cần, thêm số 0 phía trước nếu G8 có một chữ số
            g8_value = g8_value.zfill(2)

            # Dữ liệu tương ứng với từng cột trong bảng
            data_tuple = (
                row['Ngày quay xổ số'], row['Giờ xổ số'], row['Miền'], row['Tỉnh'],
                g8_value, row['G7'], row['G6'], row['G5'], row['G4'], row['G3'],
                row['G2'], row['G1'], row['ĐB'], row['draw_date'], row['draw_time']
            )

            # Thực hiện gọi stored procedure
            try:
                cursor.callproc(
                    'LS_INSERT_QUERY',  # Tên của stored procedure
                    data_tuple  # Tham số tương ứng
                )
                record_count += 1
            except Exception as e:
                print(f"Lỗi khi gọi stored procedure với dòng: {row} - {e}")

        # Commit các thay đổi
        conn.commit()

        # Đóng kết nối
        cursor.close()
        print("Dữ liệu đã được load vào bảng staging.")
        return record_count
