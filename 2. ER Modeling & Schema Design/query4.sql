SELECT ItemID
FROM Item 
WHERE Currently = (
   SELECT MAX(Currently)
   FROM Item
)