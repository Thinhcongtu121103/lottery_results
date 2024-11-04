import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime  # Import mô-đun datetime
import re

# URL của trang web cần crawl
url = 'https://xskt.com.vn/'  # URL thực tế

# Gửi yêu cầu GET đến trang web
response = requests.get(url)

# Kiểm tra xem yêu cầu có thành công không
if response.status_code == 200:
    # Phân tích cú pháp nội dung HTML
    soup = BeautifulSoup(response.text, 'html.parser')

    # Khởi tạo danh sách để lưu kết quả cho từng miền
    results = {
        'Miền Bắc': {},
        'Miền Nam': {},
        'Miền Trung': {}
    }

    # Danh sách các ID cho từng miền
    region_ids = {
        'Miền Bắc': 'MB0',
        'Miền Nam': 'MN0',
        'Miền Trung': 'MT0'
    }

    # Lấy ngày hiện tại
    now = datetime.now()  # Lấy thời gian hiện tại
    draw_date = now.strftime('%Y-%m-%d')  # Định dạng ngày
    draw_time = now.strftime('%H:%M:%S')  # Định dạng giờ

    ketqua = soup.find('div', class_='box-ketqua')
    date_text = ketqua.find('h2').text if ketqua else ''
    date_match = re.search(r'\d{2}/\d{2}', date_text)
    date = date_match.group(0) if date_match else 'Không tìm thấy ngày'
    print(date)
    # Xác định giờ quay xổ số theo từng khu vực
    times = {
        'Miền Bắc': '18:30',  # Giờ quy định cho miền Bắc
        'Miền Nam': '16:15',  # Giờ quy định cho miền Nam
        'Miền Trung': '17:15'  # Giờ quy định cho miền Trung
    }

    # Duyệt qua từng miền và tìm dữ liệu
    for region, region_id in region_ids.items():
        table = soup.find('table', id=region_id)  # Tìm bảng theo ID
        if table:
            rows = table.find_all('tr')
            if region == 'Miền Bắc':
                # Xử lý miền Bắc: có nhiều giai
                for row in rows[1:]:  # Bỏ qua hàng tiêu đề
                    cols = row.find_all('td')
                    if cols and len(cols) > 1:  # Kiểm tra xem có đủ cột không
                        giai = cols[0].get_text(strip=True)  # Lấy giai
                        numbers = cols[1].get_text(separator=', ', strip=True)
                        numbers_list = [num.strip() for num in numbers.split() if num.isdigit()]
                        formatted_numbers = ', '.join(numbers_list)  # Ghép các số bằng dấu phẩy
                        if giai == 'ĐB':
                            giai = 'ĐB'
                        else:
                            giai_number = giai[1:]  # Lấy số từ G1, G2, ...
                            giai = f"G.{giai_number}"  # Đổi thành G.X

                        # Lưu kết quả miền Bắc
                        results[region][giai] = formatted_numbers  # Lưu số cho miền Bắc
            else:
                provinces = [col.get_text(strip=True) for col in rows[0].find_all('th')[1:]]  # Dòng tiêu đề
                for row in rows[1:]:
                    cols = row.find_all('td')
                    if cols:
                        giai = cols[0].get_text(strip=True)  # Lấy giai
                        # Kiểm tra xem số lượng cột có khớp với số lượng tỉnh không
                        if len(cols) - 1 != len(provinces):  # -1 vì cột đầu tiên là giai
                            print(f"Cảnh báo: Số lượng tỉnh không khớp với dữ liệu trong hàng: {row}")
                            continue  # Bỏ qua hàng này

                        for j, col in enumerate(cols[1:]):
                            if j < len(provinces):  # Đảm bảo không vượt quá chỉ số
                                province_name = provinces[j]

                                # Lấy tất cả nội dung và thay thế <br> bằng dấu phẩy
                                numbers = col.get_text(separator=', ', strip=True)

                                # Lọc chỉ giữ lại các số
                                numbers_list = [num for num in numbers.split(', ') if num.isdigit()]
                                # Lưu kết quả
                                if province_name not in results[region]:
                                    results[region][province_name] = {}
                                # Ghi kết quả cho từng giải
                                results[region][province_name][giai] = numbers.strip()  # Lưu các số

    # Lưu kết quả vào file CSV
    csv_file_name = 'ket_qua_xo_so.csv'
    with open(csv_file_name, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)

        # Ghi tiêu đề cột
        header = ['Ngày quay xổ số', 'Giờ xổ số', 'Miền', 'Tỉnh'] + [f'G{i}' for i in range(8, 0, -1)] + ['ĐB'] + ['draw_date', 'draw_time']  # Thay đổi thứ tự cột
        writer.writerow(header)

        # Ghi dữ liệu cho miền Bắc
        row = [date, times['Miền Bắc'], 'Miền Bắc', ''] + [results['Miền Bắc'].get(f'G.{i}', '') for i in range(8, 0, -1)] + [results['Miền Bắc'].get('ĐB', '')] + [draw_date, draw_time]
        writer.writerow(row)

        # Ghi dữ liệu cho miền Nam và miền Trung
        for region, region_data in results.items():
            if region != 'Miền Bắc':  # Chỉ xử lý miền Nam và miền Trung
                for key, value in region_data.items():
                    if isinstance(value, dict):  # Nếu là từ điển
                        row = [date, times[region], region, key] + [value.get(f'G.{i}', '') for i in range(1, 9)] + [value.get('ĐB', '')] + [draw_date, draw_time]
                        writer.writerow(row)

    print(f'Dữ liệu đã được lưu vào file {csv_file_name}.')

else:
    print(f'Lỗi {response.status_code}: Không thể lấy dữ liệu từ trang web.')
