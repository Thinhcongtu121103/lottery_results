import os
import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime
import re

class DataCrawler:
    def __init__(self, url, logger, id):
        self.url = url
        self.logger = logger  # Thêm logger
        self.id = id  # ID cấu hình để ghi log
        self.results = {
            'Miền Bắc': {},
            'Miền Nam': {},
            'Miền Trung': {}
        }
        self.times = {
            'Miền Bắc': '18:30',
            'Miền Nam': '16:15',
            'Miền Trung': '17:15'
        }
    # 2.2. Phương thức tiến hành cào dữ liệu
    def crawl_data(self):
        """Gửi yêu cầu GET và phân tích cú pháp dữ liệu từ trang web."""
        try:
            self.logger.update_log(self.id, filename="", status="IN_PROGRESS", last_step="Crawl data",  total_record=0)
            response = requests.get(self.url)
            if response.status_code == 200:
                self.logger.update_log(self.id, filename="", status="SUCCESS", last_step="Fetch data from website", total_record=0)
                # 2.3. Phân tích cú pháp trang web
                self.parse_html(response.text)
            else:
                error_message = f"Lỗi {response.status_code}: Không thể lấy dữ liệu từ trang web."
                self.logger.update_log(self.id, filename="", status="FAILED", last_step="Crawl data", total_record=0)
                print(error_message)
        except Exception as e:
            self.logger.update_log(self.id, filename="", status="FAILED", last_step="Crawl data", total_record=0)
            print(f"Lỗi khi crawl dữ liệu: {e}")

    # 2.3. Phân tích cú pháp trang web
    def parse_html(self, html):
        """Phân tích cú pháp HTML và thu thập dữ liệu xổ số."""
        try:
            self.logger.update_log(self.id, filename="", status="IN_PROGRESS", last_step="Parse HTML", total_record=0)
            soup = BeautifulSoup(html, 'html.parser')
            now = datetime.now()
            draw_date = now.strftime('%Y-%m-%d')
            draw_time = now.strftime('%H:%M:%S')

            ketqua = soup.find('div', class_='box-ketqua')
            date_text = ketqua.find('h2').text if ketqua else ''
            date_match = re.search(r'\d{2}/\d{2}', date_text)
            date = date_match.group(0) if date_match else 'Không tìm thấy ngày'

            region_ids = {
                'Miền Bắc': 'MB0',
                'Miền Nam': 'MN0',
                'Miền Trung': 'MT0'
            }

            for region, region_id in region_ids.items():
                table = soup.find('table', id=region_id)
                if table:
                    rows = table.find_all('tr')
                    if region == 'Miền Bắc':
                        # 2.4. Tiến hành lấy dữ liệu miền bắc
                        self.process_mien_bac(rows)
                    else:
                        # 2.5. Tiến hành lấy dữ liệu miền nam
                        self.process_mien_nam_trung(rows, region)

            self.logger.update_log(self.id, filename="", status="SUCCESS", last_step="Parse HTML", total_record=0)
            self.save_to_csv(date, draw_date, draw_time)
        except Exception as e:
            self.logger.update_log(self.id, filename="", status="FAILED", last_step="Parse HTML", total_record=0)
            print(f"Lỗi khi phân tích HTML: {e}")

    # 2.4. Tiến hành lấy dữ liệu miền bắc
    def process_mien_bac(self, rows):
        """Xử lý dữ liệu cho miền Bắc."""
        for row in rows[1:]:
            cols = row.find_all('td')
            if cols and len(cols) > 1:
                giai = cols[0].get_text(strip=True)
                numbers = cols[1].get_text(separator=', ', strip=True)
                numbers_list = [num.strip() for num in numbers.split() if num.isdigit()]
                formatted_numbers = ', '.join(numbers_list)
                if giai == 'ĐB':
                    giai = 'ĐB'
                else:
                    giai_number = giai[1:]
                    giai = f"G.{giai_number}"

                self.results['Miền Bắc'][giai] = formatted_numbers

    # 2.5. Tiến hành lấy dữ liệu miền nam
    def process_mien_nam_trung(self, rows, region):
        """Xử lý dữ liệu cho miền Nam và miền Trung."""
        provinces = [col.get_text(strip=True) for col in rows[0].find_all('th')[1:]]
        for row in rows[1:]:
            cols = row.find_all('td')
            if cols:
                giai = cols[0].get_text(strip=True)
                if len(cols) - 1 != len(provinces):
                    print(f"Cảnh báo: Số lượng tỉnh không khớp với dữ liệu trong hàng: {row}")
                    continue

                for j, col in enumerate(cols[1:]):
                    if j < len(provinces):
                        province_name = provinces[j]
                        numbers = col.get_text(separator=', ', strip=True)
                        numbers_list = [num for num in numbers.split(', ') if num.isdigit()]
                        if province_name not in self.results[region]:
                            self.results[region][province_name] = {}
                        self.results[region][province_name][giai] = numbers.strip()

    def save_to_csv(self, date, draw_date, draw_time):
        """Lưu kết quả vào file CSV."""
        try:
            self.logger.update_log(self.id, filename="", status="IN_PROGRESS", last_step="Save to CSV", total_record=0)
            folder_path = 'ket_qua_xo_so'
            os.makedirs(folder_path, exist_ok=True)
            current_date = datetime.now().strftime('%Y%m%d')
            csv_file_name = os.path.join(folder_path, f'ket_qua_xo_so_{current_date}.csv')
            with open(csv_file_name, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)

                header = ['Ngày quay xổ số', 'Giờ xổ số', 'Miền', 'Tỉnh'] + [f'G{i}' for i in range(8, 0, -1)] + ['ĐB'] + [
                    'draw_date', 'draw_time']
                writer.writerow(header)

                # Ghi dữ liệu cho miền Bắc
                row = [date, self.times['Miền Bắc'], 'Miền Bắc', 'Rỗng'] + [self.results['Miền Bắc'].get(f'G.{i}', '') for i in
                                                                            range(8, 0, -1)] + [
                          self.results['Miền Bắc'].get('ĐB', '')] + [draw_date, draw_time]
                writer.writerow(row)

                # Ghi dữ liệu cho miền Nam và miền Trung
                for region, region_data in self.results.items():
                    if region != 'Miền Bắc':
                        for key, value in region_data.items():
                            if isinstance(value, dict):
                                row = [date, self.times[region], region, key] + [value.get(f'G.{i}', '') for i in
                                                                                 range(8, 0, -1)] + [
                                          value.get('ĐB', '')] + [draw_date, draw_time]
                                writer.writerow(row)

            self.logger.update_log(self.id, filename="", status="SUCCESS", last_step="Save to CSV", total_record=0)
            print(f'Dữ liệu đã được lưu vào file {csv_file_name}.')
        except Exception as e:
            self.logger.update_log(self.id, filename="", status="FAILED", last_step="Save to CSV", total_record=0)
            print(f"Lỗi khi lưu dữ liệu vào CSV: {e}")
