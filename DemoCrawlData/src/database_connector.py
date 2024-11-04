import mysql.connector
from mysql.connector import Error

class DatabaseConnector:
    def __init__(self, config_file):
        self.config = self.load_config(config_file)
        self.connection = None

    def load_config(self, file_path):
        config = {}
        with open(file_path, 'r') as file:
            for line in file:
                # Loại bỏ khoảng trắng và bỏ qua các dòng trống hoặc dòng bình luận
                line = line.strip()
                if line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    config[key.strip()] = value.strip()
        return config

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
            if self.connection.is_connected():
                print("Kết nối thành công đến MySQL Database")
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

# Cách sử dụng lớp DatabaseConnector
if __name__ == "__main__":
    db_connector = DatabaseConnector('config.txt')
    db_connector.connect()
    db_connector.check_connection()
    db_connector.disconnect()
