class DWLoader:
    def __init__(self, db_connector):
        self.db_connector = db_connector
        self.connection = db_connector.connection

    # 5.2. Phương thức load to DW
    def load_to_dw(self):
        # Các lệnh tạo bảng nếu chưa tồn tại



        # Bắt đầu chèn dữ liệu
        cursor = self.connection.cursor()
        cursor.callproc('dw_lottery.CreateDimAndFactTables')

        try:

            cursor.callproc('dw_lottery.LoadDataToDW')
            # Commit các thay đổi
            self.connection.commit()
            print("Dữ liệu đã được chuyển thành công vào Data Warehouse.")

        except Exception as e:
            self.connection.rollback()
            print("Lỗi khi chuyển dữ liệu vào DW:", e)
            raise  # Ném lại lỗi để main() xử lý

        finally:
            cursor.close()
