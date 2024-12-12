class ControlLogger:
    def __init__(self, db_connector):
        self.db_connector = db_connector

    def write_log(self, filename, status, last_step, extract_time, total_record=None):
        # Kết nối tới cơ sở dữ liệu
        cursor = self.db_connector.connection.cursor()

        # Tạo biến OUT cho ID trả về
        inserted_id = 0
        params = (1, filename, status, extract_time, total_record, last_step, inserted_id)

        # Gọi Stored Procedure
        result = cursor.callproc('control.InsertFileLog', params)

        # Commit thay đổi
        self.db_connector.connection.commit()

        # Lấy giá trị của ID từ biến OUT
        inserted_id = result[-1]

        cursor.close()
        return inserted_id

    def update_log(self, id, filename, status, last_step, total_record=None):
        cursor = self.db_connector.connection.cursor()
        cursor.callproc('control.UpdateFileLog', (filename, status, last_step, total_record if total_record is not None else None, id))
        self.db_connector.connection.commit()  # Đảm bảo commit vào cơ sở dữ liệu
