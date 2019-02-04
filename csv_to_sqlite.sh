rm data.db
python  compare_stops.py
sqlite3 data.db < create_table.sql
printf ".mode csv \n.import data_raw/merged.csv SCRAPPED" | sqlite3 data.db -echo




sqlite3 data.db < create_bus.sql
sqlite3 data.db < create_view.sql
