from database_connector import DatabaseConnector
from dataCrawler import DataCrawler



def main():
    # Bước 1: Kết nối database
    print("Bước 1: Kết nối database...")
    db_connector = DatabaseConnector('config.txt')
    db_connector.connect()

    # Bước 2: Crawl dữ liệu từ web
    print("Bước 2: Crawl dữ liệu từ web...")
    url = 'https://xskt.com.vn/'  # URL của trang web cần lấy dữ liệu
    dataCrawler = DataCrawler(url)
    dataCrawler.crawl_data()

    # # Bước 3: Load CSV vào bảng staging
    # print("Bước 3: Load CSV vào bảng staging...")
    # csv_path = 'ket_qua_xo_so.csv'
    # data_loader = DataLoader(db_connector, csv_path)
    # data_loader.load_to_staging()
    #
    # # Bước 4: Transform dữ liệu
    # print("Bước 4: Transform dữ liệu...")
    # transformer = DataTransformer(db_connector)
    # transformer.transform_data()
    #
    # # Bước 5: Load dữ liệu từ staging vào Data Warehouse
    # print("Bước 5: Load dữ liệu từ staging vào Data Warehouse...")
    # dw_loader = DWLoader(db_connector)
    # dw_loader.load_to_dw()
    #
    # # Đóng kết nối database
    # db_connector.disconnect()


if __name__ == "__main__":
    main()
