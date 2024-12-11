SELECT_VALUES = """
            SELECT DISTINCT dl.date_lottery, tl.time_lottery, region.region_name, province.province_name, 
                            f.g8, f.g7, f.g6, f.g5, f.g4, f.g3, f.g2, f.g1, f.db, date.draw_date, time.draw_time
            FROM fact_lottery_results f
            JOIN dim_date date ON f.date_id = date.date_id
            JOIN dim_time time ON f.time_id = time.time_id
            JOIN dim_region region ON f.region_id = region.region_id
            JOIN dim_province province ON f.province_id = province.province_id
            JOIN dim_date_lottery dl ON f.date_lottery_id = dl.date_lottery_id
            JOIN dim_time_lottery tl ON f.time_lottery_id = tl.time_lottery_id
            WHERE date.draw_date >= DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
        """
