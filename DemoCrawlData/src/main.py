import os
from datetime import datetime
import uuid  # Thêm thư viện uuid để tạo id duy nhất

from database_connector import DatabaseConnector
from dataCrawler import DataCrawler
from data_loader import DataLoader
from data_transformer import DataTransformer
from dw_loader import DWLoader
from schema_setup import create_dw_schema, check_dw_exists
from control_logger import ControlLogger


def main():
    # Bước 1: Kết nối database
    print("Bước 1: Kết nối database...")
    db_connector = DatabaseConnector('config.txt')
    db_connector.connect()
    logger = ControlLogger(db_connector)

    # Tạo id duy nhất cho lần chạy này

    # Biến trạng thái
    current_step = None
    try:
        # Ghi log ban đầu
        log_id = logger.write_log(
            filename="",
            status="STARTED",
            last_step="None",
            extract_time=datetime.now(),
        )

        # Bước 2: Crawl dữ liệu
        print("Bước 2: Crawl dữ liệu từ web...")
        current_step = "Crawl data"
        url = 'https://xskt.com.vn/'
        data_crawler = DataCrawler(url, logger, id=log_id)  # Truyền id duy nhất
        data_crawler.crawl_data()

        # Ghi log sau khi crawl dữ liệu xong
        logger.update_log(
            id=log_id,  # Cập nhật bằng id duy nhất
            status="RUNNING",
            last_step="Step 1: Crawl data",
            total_record=0,
        )

        # Bước 3: Load CSV vào bảng staging
        print("Bước 3: Load CSV vào bảng staging...")
        current_step = "Load data to staging"
        current_date = datetime.now().strftime('%Y%m%d')
        csv_path = f'ket_qua_xo_so/ket_qua_xo_so_{current_date}.csv'
        data_loader = DataLoader(db_connector, csv_path)
        total_records = data_loader.load_to_staging()

        # Ghi log sau khi load dữ liệu vào staging
        logger.update_log(
            id=log_id,  # Cập nhật bằng id duy nhất
            status="RUNNING",
            last_step="Step 2: Load data to staging",
            total_record=total_records
        )

        # Bước 4: Transform dữ liệu
        print("Bước 4: Transform dữ liệu...")
        current_step = "Transform data"
        transformer = DataTransformer(db_connector, csv_path)
        transformer.transform_and_load()

        # Ghi log sau khi transform dữ liệu
        logger.update_log(
            id=log_id,  # Cập nhật bằng id duy nhất
            status="RUNNING",
            last_step="Step 3: Transform data",
            total_record=total_records,
        )

        # Bước 5: Kiểm tra và xử lý Data Warehouse
        print("Bước 5: Kiểm tra và xử lý Data Warehouse...")
        current_step = "Process Data Warehouse"
        if not check_dw_exists(db_connector):
            create_dw_schema(db_connector)
        dw_loader = DWLoader(db_connector)
        dw_loader.load_to_dw()

        # Ghi log sau khi xử lý Data Warehouse
        logger.update_log(
            id=log_id,  # Cập nhật bằng id duy nhất
            status="RUNNING",
            last_step="Step 4: Process Data Warehouse",
            total_record=total_records,
        )

        # Ghi log hoàn tất
        logger.update_log(
            id=log_id,  # Cập nhật bằng id duy nhất
            status="COMPLETED",
            last_step="COMPLETED",
            total_record=total_records,
        )
        print("Quá trình ETL hoàn tất.")
    except Exception as e:
        # Ghi log khi xảy ra lỗi
        logger.update_log(
            id=0,  # Cập nhật bằng id duy nhất
            status="FAILED",
            last_step=current_step,
            total_record=0,
        )
        print(f"Lỗi trong quá trình thực thi tại bước '{current_step}': {e}")
    finally:
        db_connector.disconnect()


if __name__ == "__main__":
    main()
