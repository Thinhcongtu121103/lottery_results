import pandas as pd
from sql_queries import *

class DataTransformer:
    def __init__(self, db_connector, csv_path):
        self.db_connector = db_connector
        self.csv_path = csv_path

    def load_data(self):
        # Đọc dữ liệu từ CSV
        data = pd.read_csv(self.csv_path)
        return data

    # 4.3. Transform bảng dim
    def transform_and_load_dim_tables(self, data):
        # Tạo kết nối
        conn = self.db_connector.connection
        cursor = conn.cursor()

        # Tạo các bảng dim_region, dim_province, dim_date, và dim_time nếu chưa có
        cursor.callproc('create_dim_tables')

        # Load dữ liệu vào bảng dim_region
        regions = data['Miền'].dropna().unique()
        for region in regions:
            cursor.callproc('InsertDimRegion',(region,))

        # Load dữ liệu vào bảng dim_province
        provinces = data[['Tỉnh', 'Miền']].fillna({'Tỉnh': 'Rỗng'}).drop_duplicates()
        for _, row in provinces.iterrows():
            province_value = row['Tỉnh']
            # cursor.execute("SELECT region_id FROM dim_region WHERE region_name = %s", (row['Miền'],))
            # region_row = cursor.fetchone()
            # if region_row:
            #     region_id = region_row[0]
            # else:
            #     continue
            cursor.callproc('InsertDimProvince',(province_value,))

        # Load dữ liệu vào bảng dim_date
        dates = data['draw_date'].dropna().unique()
        for draw_date in dates:
            cursor.callproc('InsertDimDate', (draw_date,))

        times = data['draw_time'].dropna().unique()
        for draw_time in times:
            cursor.callproc('InsertDimTime', (draw_time,))

        date_lotterys = data['Ngày quay xổ số'].dropna().unique()
        for date_lottery in date_lotterys:
            cursor.callproc('InsertDimDateLottery', (date_lottery,))

        time_lotterys = data['Giờ xổ số'].dropna().unique()
        for time_lottery in time_lotterys:
            cursor.callproc('InsertDimTimeLottery', (time_lottery,))

        conn.commit()
        cursor.close()

    # 4.4. Load vào bảng fact
    def transform_and_load_fact_table(self, data):
        cursor = self.db_connector.connection.cursor()
        cursor.callproc('DeleteFromFactLotteryResults')
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

            region_id = 0

            # Truyền tham số dưới dạng tuple
            cursor.callproc('GetRegionIDByName', (row['Miền'],))  # Tuple chứa tham số

            # Lấy kết quả trả về từ stored procedure
            for result in cursor.stored_results():
                region_row = result.fetchone()  # Lấy dòng đầu tiên của kết quả trả về
                if region_row:
                    region_id = region_row[0]  # region_row[0] chứa giá trị region_id
                    print(region_id)  # In ra giá trị của region_id (ví dụ: 1)
                else:
                    print("Không tìm thấy region_id.")
            # Lấy province_id từ bảng dim_province
            cursor.execute("SELECT province_id FROM dim_province WHERE province_name = %s", (row['Tỉnh'],))
            province_row = cursor.fetchone()
            province_id = province_row[0] if province_row else Noneregion_id = 0

            # Truyền tham số dưới dạng tuple
            cursor.callproc('GetRegionIDByName', (row['Miền'],))  # Tuple chứa tham số

            # Lấy kết quả trả về từ stored procedure
            for result in cursor.stored_results():
                region_row = result.fetchone()  # Lấy dòng đầu tiên của kết quả trả về
                if region_row:
                    region_id = region_row[0]  # region_row[0] chứa giá trị region_id
                    print(region_id)  # In ra giá trị của region_id (ví dụ: 1)
                else:
                    print("Không tìm thấy region_id.")

            # Lấy date_id từ bảng dim_date
            draw_date = row['draw_date']
            cursor.execute("SELECT date_id FROM dim_date WHERE draw_date = %s", (draw_date,))
            date_row = cursor.fetchone()
            date_id = date_row[0] if date_row else None

            date_lottery = row['Ngày quay xổ số']
            cursor.execute("SELECT date_lottery_id FROM dim_date_lottery WHERE date_lottery = %s", (date_lottery,))
            date_lottery_row = cursor.fetchone()
            date_lottery_id = date_lottery_row[0] if date_lottery_row else None

            time_lottery = row['Giờ xổ số']
            cursor.execute("SELECT time_lottery_id FROM dim_time_lottery WHERE time_lottery = %s", (time_lottery,))
            time_lottery_row = cursor.fetchone()
            time_lottery_id = time_lottery_row[0] if time_lottery_row else None

            # Lấy time_id từ bảng dim_time
            draw_time = row['draw_time']
            cursor.execute("SELECT time_id FROM dim_time WHERE draw_time = %s", (draw_time,))
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
                    INSERT INTO fact_lottery_results (date_lottery_id, time_lottery_id, date_id, time_id, region_id, province_id, g8, g7, g6, g5, g4, g3, g2, g1, db)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        date_lottery_id, time_lottery_id, date_id, time_id, region_id, province_id,
                        g8_value, row['G7'], row['G6'], row['G5'], row['G4'],
                        row['G3'], row['G2'], row['G1'], row['ĐB']
                    )
                )
            else:
                print(f"Bỏ qua bản ghi do thiếu khóa ngoại: {row}")

        # Lưu thay đổi và đóng cursor
        self.db_connector.connection.commit()
        cursor.close()

    # 4.2. Phương thức transform và load vô table mới
    def transform_and_load(self):
        data = self.load_data()
        # 4.3. Transform bảng dim
        self.transform_and_load_dim_tables(data)
        # 4.4. Load vào bảng fact
        self.transform_and_load_fact_table(data)
        print("Dữ liệu đã được transform và load vào bảng staging thành công.")
