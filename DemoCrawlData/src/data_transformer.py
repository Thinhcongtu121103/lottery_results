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

        # Tạo các bảng dim_region và dim_province nếu chưa có
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS dim_region (region_id INT PRIMARY KEY AUTO_INCREMENT, region_name VARCHAR(50) UNIQUE)")
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS dim_province (province_id INT PRIMARY KEY AUTO_INCREMENT, province_name VARCHAR(100) UNIQUE, region_id INT, FOREIGN KEY (region_id) REFERENCES dim_region(region_id))")

        # Load dữ liệu vào bảng dim_region
        regions = data['Miền'].dropna().unique()
        for region in regions:
            cursor.execute("INSERT IGNORE INTO dim_region (region_name) VALUES (%s)", (region,))

        # Đảm bảo "Miền Bắc" luôn có region_id = 1
        cursor.execute("SELECT region_id FROM dim_region WHERE region_name = %s", ("Miền Bắc",))
        region_row = cursor.fetchone()
        if not region_row:  # Nếu không có "Miền Bắc", chèn mới
            cursor.execute("INSERT INTO dim_region (region_name) VALUES (%s)", ("Miền Bắc",))
            region_id = 1  # Gán region_id = 1 cho "Miền Bắc"
        else:
            region_id = region_row[0]  # Lấy region_id nếu đã tồn tại

        # Load dữ liệu vào bảng dim_province
        provinces = data[['Tỉnh', 'Miền']].fillna({'Tỉnh': 'Rỗng'}).drop_duplicates()

        print(provinces)
        for _, row in provinces.iterrows():
            province_value = row['Tỉnh']
            cursor.execute("SELECT region_id FROM dim_region WHERE region_name = %s", (row['Miền'],))
            region_row = cursor.fetchone()
            print(region_row)
            # Nếu không có region_id cho miền, và miền là "Miền Bắc", tạo region_id = 1
            if region_row:
                region_id = region_row[0]
            else:
                if row['Miền'] == "Miền Bắc":
                    # Gán lại region_id là 1 cho Miền Bắc nếu không tìm thấy
                    cursor.execute("INSERT INTO dim_region (region_name) VALUES (%s)", ("Miền Bắc",))
                    region_id = 1

                else:
                    continue  # Bỏ qua nếu không thể tìm thấy region_id hợp lệ

            # Chèn tỉnh vào dim_province với region_id
            cursor.execute("INSERT IGNORE INTO dim_province (province_name, region_id) VALUES (%s, %s)",
                           (province_value, region_id))
        # Lưu thay đổi và đóng cursor
        conn.commit()
        cursor.close()

    def transform_and_load_fact_table(self, data):
        cursor = self.db_connector.connection.cursor()

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
            province_id = province_row[0] if province_row else 0  # Gán province_id = 0 nếu không tìm thấy

            # Xử lý giá trị 'g8' cho miền Bắc và loại bỏ dấu .0 nếu có
            g8_value = row['G8']

            if pd.isna(g8_value):  # Kiểm tra nếu giá trị G8 là NaN
                g8_value = ""  # Hoặc gán giá trị mặc định khác như "00"
            elif isinstance(g8_value, float):  # Nếu giá trị là float
                g8_value = str(int(g8_value))  # Loại bỏ .0 và chuyển thành chuỗi
            # Nếu cần, thêm số 0 phía trước nếu G8 có một chữ số
            g8_value = g8_value.zfill(2)  # Ép kiểu thành int để loại bỏ .0

            # Nếu region_id và province_id đều tồn tại, chèn dữ liệu vào bảng fact
            if region_id is not None and province_id is not None:
                cursor.execute(
                    """
                    INSERT INTO fact_lottery_results (draw_date, draw_time, region_id, province_id, g8, g7, g6, g5, g4, g3, g2, g1, db)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        row['draw_date'], row['draw_time'], region_id, province_id,
                        g8_value, row['G7'], row['G6'], row['G5'], row['G4'],
                        row['G3'], row['G2'], row['G1'], row['ĐB']
                    )
                )
            else:
                print(f"Bỏ qua bản ghi do thiếu region_id hoặc province_id: {row}")

        # Lưu thay đổi và đóng cursor
        self.db_connector.connection.commit()
        cursor.close()

    def transform_and_load(self):
        data = self.load_data()
        self.transform_and_load_dim_tables(data)
        self.transform_and_load_fact_table(data)
        print("Dữ liệu đã được transform và load vào bảng staging thành công.")
