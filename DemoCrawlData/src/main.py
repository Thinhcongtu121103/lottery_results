# main.py

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
from send_email import SendEmail  # Import class SendEmail


def main():
    # Biến điều khiển gửi mail
    send_mail_status = False  # Đặt True để bật gửi email, False để tắt

    # Khởi tạo đối tượng gửi email
    email_notifier = SendEmail(
        sender_email="thinhcongtu121103@gmail.com",
        sender_password="hmxn qiou vros uuzm",
        smtp_server="smtp.gmail.com",
        smtp_port=587,
    )
    recipient_email = "21130187@st.hcmuaf.edu.vn"

    # Bước 1: Kết nối database
    print("Bước 1: Kết nối database...")
    db_connector = DatabaseConnector('config.txt')
    db_connector.connect()
    logger = ControlLogger(db_connector)

    current_step = None
    log_id = None  # Biến log_id để theo dõi log
    try:
        # Ghi log ban đầu
        log_id = logger.write_log(
            filename="",
            status="Step 1: Connect database",
            last_step="None",
            extract_time=datetime.now(),
        )

        # Bước 2: Crawl dữ liệu
        print("Bước 2: Crawl dữ liệu từ web...")
        current_step = "Crawl data"
        url = 'https://xskt.com.vn/'
        data_crawler = DataCrawler(url, logger, id=log_id)
        data_crawler.crawl_data()

        logger.update_log(
            id=log_id,
            status="RUNNING",
            last_step="Step 2: Crawl data",
            total_record=0,
        )

        # Bước 3: Load CSV vào bảng staging
        print("Bước 3: Load CSV vào bảng staging...")
        current_step = "Load data to staging"
        current_date = datetime.now().strftime('%Y%m%d')
        csv_path = f'ket_qua_xo_so/ket_qua_xo_so_{current_date}.csv'
        data_loader = DataLoader(db_connector, csv_path)
        total_records = data_loader.load_to_staging()

        logger.update_log(
            id=log_id,
            status="RUNNING",
            last_step="Step 3: Load data to staging",
            total_record=total_records
        )

        # Bước 4: Transform dữ liệu
        print("Bước 4: Transform dữ liệu...")
        current_step = "Transform data"
        transformer = DataTransformer(db_connector, csv_path)
        transformer.transform_and_load()

        logger.update_log(
            id=log_id,
            status="RUNNING",
            last_step="Step 4: Transform data",
            total_record=total_records,
        )

        # Bước 5: Kiểm tra và xử lý Data Warehouse
        print("Bước 5: Kiểm tra và xử lý Data Warehouse...")
        current_step = "Process Data Warehouse"
        if not check_dw_exists(db_connector):
            create_dw_schema(db_connector)
        dw_loader = DWLoader(db_connector)
        dw_loader.load_to_dw()

        logger.update_log(
            id=log_id,
            status="RUNNING",
            last_step="Step 5: Process Data Warehouse",
            total_record=total_records,
        )

        # Ghi log hoàn tất
        logger.update_log(
            id=log_id,
            status="COMPLETED",
            last_step="COMPLETED",
            total_record=total_records,
        )

        # Gửi email khi hoàn tất (nếu bật gửi mail)
        if send_mail_status:
            email_notifier.send_email(
                to=recipient_email,
                subject="ETL Process - Completed",
                body="The entire ETL process has been successfully completed.",
            )
        print("Quá trình ETL hoàn tất.")
    except Exception as e:
        error_message = str(e)
        print(f"Lỗi trong quá trình thực thi tại bước '{current_step}': {error_message}")

        # Cập nhật log lỗi với trạng thái FAILED
        if log_id:
            logger.update_log(
                id=log_id,
                status="FAILED",
                last_step=current_step,
                total_record=0,
            )

        # Gửi email thông báo lỗi (nếu bật gửi mail)
        if send_mail_status:
            email_notifier.send_email(
                to=recipient_email,
                subject="ETL Process - Failed",
                body=f"ETL process failed at step '{current_step}'. Error: {error_message}",
            )
    finally:
        db_connector.disconnect()


if __name__ == "__main__":
    main()
