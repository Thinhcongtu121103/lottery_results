from flask import Flask, render_template
import pymysql

from src import queries

app = Flask(__name__)

db_config = {
    'host': 'localhost',
    'user': 'root',
    'port': 3306,
    'password': '',
    'database': 'dw_lottery'
}

@app.route('/')
def index():
    try:
        # Kết nối tới database
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()

        # Truy vấn dữ liệu

        cursor.execute(queries.SELECT_VALUES)
        results = cursor.fetchall()

    except pymysql.MySQLError as e:
        return f"Lỗi kết nối hoặc truy vấn cơ sở dữ liệu: {e}"

    finally:
        # Đảm bảo đóng kết nối
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return render_template('index.html', results=results)

if __name__ == '__main__':
    app.run(debug=True)
