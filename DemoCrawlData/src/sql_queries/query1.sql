-- CREATE TABLE statements for Dimension Tables (DIM)
-- name: CREATE_DIM_REGION
CREATE TABLE IF NOT EXISTS dim_region (
    region_id INT AUTO_INCREMENT PRIMARY KEY,
    region_name VARCHAR(255) NOT NULL
);

-- Tạo bảng Dim Province
CREATE TABLE IF NOT EXISTS dim_province (
    province_id INT AUTO_INCREMENT PRIMARY KEY,
    province_name VARCHAR(255) NOT NULL,
    region_id INT,
    FOREIGN KEY (region_id) REFERENCES dim_region(region_id)
);

-- Tạo bảng Dim Date
CREATE TABLE IF NOT EXISTS dim_date (
    date_id INT AUTO_INCREMENT PRIMARY KEY,
    date_value DATE NOT NULL
);

-- Tạo bảng Dim Time
CREATE TABLE IF NOT EXISTS dim_time (
    time_id INT AUTO_INCREMENT PRIMARY KEY,
    time_value TIME NOT NULL
);

-- Tạo bảng Dim Date Lottery
CREATE TABLE IF NOT EXISTS dim_date_lottery (
    date_lottery_id INT AUTO_INCREMENT PRIMARY KEY,
    lottery_date DATE NOT NULL
);

-- Tạo bảng Dim Time Lottery
CREATE TABLE IF NOT EXISTS dim_time_lottery (
    time_lottery_id INT AUTO_INCREMENT PRIMARY KEY,
    lottery_time TIME NOT NULL
);

-- CREATE TABLE statement for Fact Table (FACT)
-- Tạo bảng Fact Lottery Results
CREATE TABLE IF NOT EXISTS fact_lottery_results (
    fact_id INT AUTO_INCREMENT PRIMARY KEY,
    date_lottery_id INT,
    time_lottery_id INT,
    date_id INT,
    time_id INT,
    region_id INT,
    province_id INT,
    g8 INT,
    g7 INT,
    g6 INT,
    g5 INT,
    g4 INT,
    g3 INT,
    g2 INT,
    g1 INT,
    db INT,
    FOREIGN KEY (date_lottery_id) REFERENCES dim_date_lottery(date_lottery_id),
    FOREIGN KEY (time_lottery_id) REFERENCES dim_time_lottery(time_lottery_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (region_id) REFERENCES dim_region(region_id),
    FOREIGN KEY (province_id) REFERENCES dim_province(province_id)
);

-- INSERT statements for Dimension Tables
-- Chèn dữ liệu vào bảng dim_region
INSERT INTO dim_region (region_name)
VALUES (%s);

-- Chèn dữ liệu vào bảng dim_province
INSERT INTO dim_province (province_name, region_id)
VALUES (%s, %s);

-- Chèn dữ liệu vào bảng dim_date
INSERT INTO dim_date (date_value)
VALUES (%s);

-- Chèn dữ liệu vào bảng dim_time
INSERT INTO dim_time (time_value)
VALUES (%s);

-- Chèn dữ liệu vào bảng dim_date_lottery
INSERT INTO dim_date_lottery (lottery_date)
VALUES (%s);

-- Chèn dữ liệu vào bảng dim_time_lottery
INSERT INTO dim_time_lottery (lottery_time)
VALUES (%s);

-- INSERT statement for Fact Table
-- Chèn dữ liệu vào bảng fact_lottery_results
INSERT INTO fact_lottery_results (
    date_lottery_id, time_lottery_id, date_id, time_id, region_id, province_id,
    g8, g7, g6, g5, g4, g3, g2, g1, db
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);

-- SELECT statements to get IDs from Dimension Tables
-- Lấy ID của dim_region
SELECT region_id FROM dim_region WHERE region_name = %s;

-- Lấy ID của dim_province
SELECT province_id FROM dim_province WHERE province_name = %s;

-- Lấy ID của dim_date
SELECT date_id FROM dim_date WHERE date_value = %s;

-- Lấy ID của dim_time
SELECT time_id FROM dim_time WHERE time_value = %s;

-- Lấy ID của dim_date_lottery
SELECT date_lottery_id FROM dim_date_lottery WHERE lottery_date = %s;

-- Lấy ID của dim_time_lottery
SELECT time_lottery_id FROM dim_time_lottery WHERE lottery_time = %s;
