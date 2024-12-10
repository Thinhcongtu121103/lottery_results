import pandas as pd


class DataTransformer:
    def __init__(self, db_connector, csv_path):
        self.db_connector = db_connector
        self.csv_path = csv_path

    def load_data(self):
        # Đọc dữ liệu từ CSV
        data = pd.read_csv(self.csv_path)
        return data

    def transform_and_load_dim_tables(self, data):
        # Tạo kết nối
        conn = self.db_connector.connection
        cursor = conn.cursor()

        # Tạo các bảng dim_region, dim_province, dim_date, và dim_time nếu chưa có
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS dim_region (region_id INT PRIMARY KEY AUTO_INCREMENT, region_name VARCHAR(50) UNIQUE)")
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS dim_province (province_id INT PRIMARY KEY AUTO_INCREMENT, province_name VARCHAR(100) UNIQUE, region_id INT, FOREIGN KEY (region_id) REFERENCES dim_region(region_id))")
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS dim_date (date_id INT PRIMARY KEY AUTO_INCREMENT, draw_date DATE UNIQUE)")
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS dim_time (time_id INT PRIMARY KEY AUTO_INCREMENT, time_period VARCHAR(20) UNIQUE)")
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS dim_date_lottery (date_lottery_id INT PRIMARY KEY AUTO_INCREMENT, date_lottery VARCHAR(20) UNIQUE)")

        # Load dữ liệu vào bảng dim_region
        regions = data['Miền'].dropna().unique()
        for region in regions:
            cursor.execute("INSERT IGNORE INTO dim_region (region_name) VALUES (%s)", (region,))

        # Load dữ liệu vào bảng dim_province
        provinces = data[['Tỉnh', 'Miền']].fillna({'Tỉnh': 'Rỗng'}).drop_duplicates()
        for _, row in provinces.iterrows():
            province_value = row['Tỉnh']
            cursor.execute("SELECT region_id FROM dim_region WHERE region_name = %s", (row['Miền'],))
            region_row = cursor.fetchone()
            if region_row:
                region_id = region_row[0]
            else:
                continue
            cursor.execute("INSERT IGNORE INTO dim_province (province_name, region_id) VALUES (%s, %s)",
                           (province_value, region_id))

        # Load dữ liệu vào bảng dim_date
        dates = data['draw_date'].dropna().unique()
        for draw_date in dates:
            cursor.execute("INSERT IGNORE INTO dim_date (draw_date) VALUES (%s)", (draw_date,))

        date_lotterys = data['Ngày quay xổ số'].dropna().unique()
        for date_lottery in date_lotterys:
            cursor.execute("INSERT IGNORE INTO dim_date_lottery (date_lottery) VALUES (%s)", (date_lottery,))

        def determine_time_period(time):
            if "05:00:00" <= time <= "11:59:59":
                return "Sáng"
            elif "12:00:00" <= time <= "17:59:59":
                return "Chiều"
            elif "18:00:00" <= time <= "21:59:59":
                return "Tối"
            else:
                return None
        # Load dữ liệu vào bảng dim_time với các giá trị khung giờ duy nhất
        draw_times = data['draw_time'].dropna().unique()
        for draw_time in draw_times:
            time_period = determine_time_period(draw_time)

            # Bỏ qua nếu không thuộc khung giờ nào
            if not time_period:
                continue

            # Kiểm tra xem draw_time đã tồn tại trong bảng dim_time chưa
            cursor.execute("SELECT * FROM dim_time WHERE time_period = %s", (time_period,))
            existing_row = cursor.fetchone()

            # Nếu chưa tồn tại, chèn vào bảng
            if not existing_row:
                cursor.execute("INSERT INTO dim_time (time_period) VALUES (%s)", (time_period,))
        conn.commit()
        cursor.close()

    def transform_and_load_fact_table(self, data):
        cursor = self.db_connector.connection.cursor()

        # Hàm xác định khung giờ
        def determine_time_period(draw_time):
            if 5 <= draw_time <= 11:  # Từ 5 giờ sáng đến 11 giờ sáng
                return "Sáng"
            elif 12 <= draw_time <= 17:  # Từ 12 giờ trưa đến 17 giờ chiều
                return "Chiều"
            elif 18 <= draw_time <= 21:  # Từ 18 giờ tối đến 21 giờ tối
                return "Tối"
            else:
                return None

        for _, row in data.iterrows():
            # Kiểm tra và xử lý giá trị NaN trong cột 'Tỉnh' và 'Miền'
            if pd.isna(row['Tỉnh']):
                print(f"Bỏ qua bản ghi do thiếu giá trị Tỉnh: {row}")
                continue

            if pd.isna(row['Miền']):
                print(f"Bỏ qua bản ghi do thiếu giá trị Miền: {row}")
                continue

            # Lấy region_id từ bảng dim_region
            cursor.execute("SELECT region_id FROM dim_region WHERE region_name = %s", (row['Miền'],))
            region_row = cursor.fetchone()
            region_id = region_row[0] if region_row else None

            # Lấy province_id từ bảng dim_province
            cursor.execute("SELECT province_id FROM dim_province WHERE province_name = %s", (row['Tỉnh'],))
            province_row = cursor.fetchone()
            province_id = province_row[0] if province_row else None

            # Lấy date_id từ bảng dim_date
            draw_date = row['draw_date']
            cursor.execute("SELECT date_id FROM dim_date WHERE draw_date = %s", (draw_date,))
            date_row = cursor.fetchone()
            date_id = date_row[0] if date_row else None

            date_lottery = row['Ngày quay xổ số']
            cursor.execute("SELECT date_lottery_id FROM dim_date_lottery WHERE date_lottery = %s", (date_lottery,))
            date_lottery_row = cursor.fetchone()
            date_lottery_id = date_lottery_row[0] if date_lottery_row else None

            # Xử lý draw_time (chuỗi dạng HH:MM:SS -> số giờ)
            draw_time_str = row['draw_time']
            try:
                draw_hour = int(draw_time_str.split(":")[0])  # Tách và lấy giờ
            except ValueError:
                print(f"Bỏ qua bản ghi do draw_time không hợp lệ: {row}")
                continue

            # Xác định khung giờ từ draw_hour
            time_period = determine_time_period(draw_hour)
            if not time_period:  # Bỏ qua nếu không thuộc khung giờ nào
                print(f"Bỏ qua bản ghi do draw_time không hợp lệ: {row}")
                continue

            # Lấy time_id từ bảng dim_time
            cursor.execute("SELECT time_id FROM dim_time WHERE time_period = %s", (time_period,))
            time_row = cursor.fetchone()
            time_id = time_row[0] if time_row else None

            # Xử lý giá trị 'g8' cho miền Bắc và loại bỏ dấu .0 nếu có
            g8_value = row['G8']

            if pd.isna(g8_value):  # Kiểm tra nếu giá trị G8 là NaN
                g8_value = ""  # Hoặc gán giá trị mặc định khác như "00"
            elif isinstance(g8_value, float):  # Nếu giá trị là float
                g8_value = str(int(g8_value))  # Loại bỏ .0 và chuyển thành chuỗi
            g8_value = g8_value.zfill(2)  # Thêm số 0 nếu cần

            # Nếu tất cả khóa ngoại tồn tại, chèn dữ liệu vào bảng fact_lottery_results
            if region_id and province_id and date_id and time_id and date_lottery_id:
                cursor.execute(
                    """
                    INSERT INTO fact_lottery_results (date_lottery_id, date_id, time_id, region_id, province_id, g8, g7, g6, g5, g4, g3, g2, g1, db)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        date_lottery_id, date_id, time_id, region_id, province_id,
                        g8_value, row['G7'], row['G6'], row['G5'], row['G4'],
                        row['G3'], row['G2'], row['G1'], row['ĐB']
                    )
                )
            else:
                print(f"Bỏ qua bản ghi do thiếu khóa ngoại: {row}")

        # Lưu thay đổi và đóng cursor
        self.db_connector.connection.commit()
        cursor.close()

    def transform_and_load(self):
        data = self.load_data()
        self.transform_and_load_dim_tables(data)
        self.transform_and_load_fact_table(data)
        print("Dữ liệu đã được transform và load vào bảng staging thành công.")
