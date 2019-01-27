DROP VIEW BUS_INFO;
CREATE VIEW BUS_INFO AS
SELECT id_bus, COUNT(Bus.id) as nb_data,
CASE WHEN MIN(minutes)<2 THEN MAX(time_saved) ELSE NULL END AS arrivee,
MAX(minutes)  as minutes_max
FROM BUS INNER JOIN SCRAPPED ON BUS.id = SCRAPPED.id
GROUP BY id_bus;
