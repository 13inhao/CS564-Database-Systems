SELECT COUNT(DISTINCT User.UserID)
FROM User, Item 
WHERE User.UserID = Item.SellerID AND User.Rating > 1000;
