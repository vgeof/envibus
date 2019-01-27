#! /bin/sh
echo "DROP TABLE SCRAPPED;"| sqlite3 recent.db 
echo "DROP TABLE BUS;"| sqlite3 recent.db 
echo "DROP VIEW BUS_INFO"|sqlite3 recent.db
echo '.schema' | sqlite3 data.db | sqlite3 recent.db
printf ".mode insert SCRAPPED\n SELECT * FROM SCRAPPED WHERE time_saved>datetime('now','-1 hours');" | sqlite3 data.db|sed '1 iBEGIN TRANSACTION;'|sed -e "\$aCOMMIT;" | sqlite3 recent.db
chmod 666 recent.db

#sqlite3 recent.db < create_view.sql
