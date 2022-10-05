SELECT COUNT(DISTINCT Belong.CategoryID)
FROM Belong
WHERE Belong.ItemID in (
    SELECT DISTINCT Bid.ItemID
    FROM Bid
    WHERE Bid.Amount > 100);
