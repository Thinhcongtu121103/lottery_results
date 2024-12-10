import mysql.connector
from mysql.connector import Error


class DatabaseConnector:
    def __init__(self, config_file):
        # 1.2. Đọc file config.txt
        self.config = self.load_config(config_file)
        self.connection = None

    # 1.2. Method đọc file
    def load_config(self, file_path):
        """Load cấu hình từ file config."""
        config = {}
        with open(file_path, 'r') as file:
            for line in file:
                # Loại bỏ khoảng trắng và bỏ qua các dòng trống hoặc dòng bình luận
                line = line.strip()
                if line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    config[key.strip()] = value.strip()
        return config

    # 1.3 Khởi tạo kết nối MySQL
    def connect(self):
        """Kết nối vào cơ sở dữ liệu MySQL."""
        try:
            self.connection = mysql.connector.connect(
                host=self.config.get("host"),
                port=self.config.get("port"),
                user=self.config.get("username"),
                password=self.config.get("password"),
                database=self.config.get("database")
            )
            # 1.3.1. Kết nối thành công
            if self.connection.is_connected():
                print("Kết nối thành công đến MySQL Database")
        # 1.3.2 Kết nối thất bại. Print lỗi
        except Error as e:
            print("Lỗi kết nối:", e)

    def disconnect(self):
        """Ngắt kết nối khỏi cơ sở dữ liệu MySQL."""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("Đã ngắt kết nối khỏi MySQL Database")

    def check_connection(self):
        """Kiểm tra xem có kết nối hay không."""
        if self.connection and self.connection.is_connected():
            print("Kết nối vẫn còn hoạt động.")
        else:
            print("Không có kết nối.")

    def execute_query(self, query, values=None):
        """Thực thi một câu lệnh SQL (INSERT, UPDATE, DELETE, SELECT)."""
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, values) if values else cursor.execute(query)
            self.connection.commit()
            if query.lower().startswith("select"):
                return cursor.fetchall()  # Trả về kết quả truy vấn SELECT
            else:
                return cursor.rowcount  # Trả về số lượng bản ghi bị thay đổi
        except Error as e:
            print(f"Lỗi khi thực thi câu lệnh SQL: {e}")
            self.connection.rollback()  # Rollback nếu có lỗi
        finally:
            cursor.close()


# Cách sử dụng lớp DatabaseConnector với execute_query
if __name__ == "__main__":
    db_connector = DatabaseConnector('config.txt')
    db_connector.connect()
    db_connector.disconnect()
