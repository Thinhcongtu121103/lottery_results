import mysql.connector
from mysql.connector import Error

def load_config(file_path):
    config = {}
    with open(file_path, 'r') as file:
        for line in file:
            # Loại bỏ khoảng trắng và bỏ qua các dòng trống hoặc dòng bình luận
            line = line.strip()
            if line and not line.startswith('#'):
                key, value = line.split('=', 1)
                config[key.strip()] = value.strip()
    return config

# Đọc file config.txt
config = load_config('config.txt')

# Kết nối vào database bằng cách sử dụng các thông tin đã đọc
print("Username:", config.get("username"))
print("Password:", config.get("password"))
print("Email:", config.get("email"))
print("Host:", config.get("host"))
print("Port:", config.get("port"))
print("Database:", config.get("database"))

def check_mysql_connection(config):
    try:
        connection = mysql.connector.connect(
            host=config.get("host"),
            port=config.get("port"),
            user=config.get("username"),
            password=config.get("password"),
            database=config.get("database")
        )
        if connection.is_connected():
            print("Kết nối thành công đến MySQL Database")
    except Error as e:
        print("Lỗi kết nối:", e)
    finally:
        if connection.is_connected():
            connection.close()

# Load config từ file config.txt và kiểm tra kết nối
config = load_config('config.txt')
check_mysql_connection(config)
