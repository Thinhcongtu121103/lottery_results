import os
from typing import Dict

class SQLProducer:
    def __init__(self, sql_folder: str):
        """
        Khởi tạo SQLProducer để quản lý các câu lệnh SQL từ thư mục.

        Args:
            sql_folder (str): Đường dẫn đến thư mục chứa các file SQL.
        """
        self.sql_folder = sql_folder
        self.queries = {}  # Lưu trữ các câu lệnh SQL

    def load_queries(self, file_name: str):
        """
        Đọc và lưu trữ các câu lệnh SQL từ file.

        Args:
            file_name (str): Tên file SQL.
        """
        file_path = os.path.join(self.sql_folder, file_name)
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                content = file.read()
        except FileNotFoundError:
            raise FileNotFoundError(f"File SQL '{file_name}' không tồn tại.")

        # Tách các câu lệnh SQL theo định danh
        queries = content.split('-- name:')
        for query in queries:
            if query.strip():  # Bỏ qua các phần rỗng
                lines = query.strip().split('\n')
                query_name = lines[0].strip()  # Lấy tên câu lệnh
                sql = '\n'.join(lines[1:]).strip()  # Lấy nội dung SQL
                self.queries[query_name] = sql

    def get_query(self, query_name: str) -> str:
        """
        Lấy câu lệnh SQL theo tên.

        Args:
            query_name (str): Tên câu lệnh SQL.

        Returns:
            str: Câu lệnh SQL tương ứng.
        """
        if query_name in self.queries:
            return self.queries[query_name]
        raise ValueError(f"Câu lệnh SQL '{query_name}' không tồn tại.")
