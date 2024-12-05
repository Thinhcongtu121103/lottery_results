class ControlLogger:
    def __init__(self, db_connector):
        self.db_connector = db_connector

    def write_log(self, filename, status, last_step, extract_time, total_record=None):
        # Tạo câu truy vấn SQL để chèn log mới vào cơ sở dữ liệu
        cursor = self.db_connector.connection.cursor()
        query = """
        INSERT INTO control.file_logs (id_config, filename, status, extract_time, total_record, last_step)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        values = (1, filename, status, extract_time, total_record, last_step)

        cursor.execute(query, values)
        self.db_connector.connection.commit()  # Đảm bảo commit vào cơ sở dữ liệu
        return cursor.lastrowid  # Trả về ID tự động tăng của bản ghi vừa thêm

    def update_log(self, id, status, last_step, total_record=None):
        # Xây dựng câu truy vấn SQL tùy thuộc vào việc có total_record hay không
        query = """
        UPDATE control.file_logs
        SET status = %s, last_step = %s, extract_time = NOW(), total_record = %s
        WHERE id = %s
        """
        # Nếu không có total_record, truyền NULL cho trường này
        values = (status, last_step, total_record if total_record is not None else None, id)

        cursor = self.db_connector.connection.cursor()
        cursor.execute(query, values)
        self.db_connector.connection.commit()  # Đảm bảo commit vào cơ sở dữ liệu
