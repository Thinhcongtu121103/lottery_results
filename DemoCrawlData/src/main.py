import os

from database_connector import DatabaseConnector
from dataCrawler import DataCrawler
from data_loader import DataLoader
from data_transformer import DataTransformer
# Giả định rằng DWLoader đã được tạo để load dữ liệu từ staging vào Data Warehouse
from dw_loader import DWLoader
from schema_setup import create_dw_schema, check_dw_exists
from datetime import datetime

def main():
    # Bước 1: Kết nối database
    print("Bước 1: Kết nối database...")
    db_connector = DatabaseConnector('config.txt')
    db_connector.connect()

    # Bước 2: Crawl dữ liệu từ web
    print("Bước 2: Crawl dữ liệu từ web...")
    url = 'https://xskt.com.vn/'  # URL của trang web cần lấy dữ liệu
    data_crawler = DataCrawler(url)
    data_crawler.crawl_data()

    # Bước 3: Load CSV vào bảng staging
    print("Bước 3: Load CSV vào bảng staging...")
    current_date = datetime.now().strftime('%Y%m%d')
    folder_path = 'ket_qua_xo_so'  # Thay đổi thành tên thư mục bạn đã lưu file
    csv_path = os.path.join(folder_path, f'ket_qua_xo_so_{current_date}.csv')  # Kết hợp đường dẫn thư mục và tên file

    # Kiểm tra file có tồn tại hay không trước khi đọc
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"File {csv_path} không tồn tại. Vui lòng kiểm tra lại.")

    # Load dữ liệu từ file CSV
    data_loader = DataLoader(db_connector, csv_path)
    data_loader.load_to_staging()

    # Bước 4: Transform dữ liệu và load vào các bảng dim và fact
    print("Bước 4: Transform dữ liệu...")
    transformer = DataTransformer(db_connector, csv_path)
    transformer.transform_and_load()

    # # Bước 5: Load dữ liệu từ staging vào Data Warehouse
    print("Bước 5: Kiểm tra và xử lý Data Warehouse...")
    if not check_dw_exists(db_connector):
        print("Data Warehouse chưa tồn tại. Bắt đầu tạo mới...")
        create_dw_schema(db_connector)  # Tạo schema Data Warehouse
    else:
        print("Data Warehouse đã tồn tại. Bỏ qua bước tạo schema.")

    # Load dữ liệu từ staging vào DW
    dw_loader = DWLoader(db_connector)
    dw_loader.load_to_dw()

    # Đóng kết nối database
    db_connector.disconnect()
    print("Quá trình ETL hoàn tất.")

if __name__ == "__main__":
    main()
