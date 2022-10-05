SELECT COUNT(*)
FROM (
    SELECT ItemID, COUNT(CategoryID) AS num
    FROM Belong
    GROUP BY ItemID
    HAVING num = 4
);