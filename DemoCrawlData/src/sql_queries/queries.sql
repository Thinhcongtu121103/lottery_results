-- name: insert_lottery_results
INSERT INTO lottery_results (
    ngay_quay_xo_so, gio_xo_so, mien, tinh, g8, g7, g6, g5, g4, g3, g2, g1, db, draw_date, draw_time
) VALUES (
    %(ngay_quay_xo_so)s, %(gio_xo_so)s, %(mien)s, %(tinh)s, %(g8)s, %(g7)s, %(g6)s, %(g5)s, %(g4)s,
    %(g3)s, %(g2)s, %(g1)s, %(db)s, %(draw_date)s, %(draw_time)s
);


