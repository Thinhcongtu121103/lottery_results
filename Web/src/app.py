from flask import Flask, render_template
import pymysql

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
        query = """
        SELECT dl.date_lottery, tl.time_lottery, region.region_name, province.province_name, 
               f.g8, f.g7, f.g6, f.g5, f.g4, f.g3, f.g2, f.g1, f.db
        FROM fact_lottery_results f
        JOIN dim_date date ON f.date_id = date.date_id
        JOIN dim_time time ON f.time_id = time.time_id
        JOIN dim_region region ON f.region_id = region.region_id
        JOIN dim_province province ON f.province_id = province.province_id
        JOIN dim_date_lottery dl ON f.date_lottery_id = dl.date_lottery_id
        JOIN dim_time_lottery tl ON f.time_lottery_id = tl.time_lottery_id
        """
        cursor.execute(query)
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