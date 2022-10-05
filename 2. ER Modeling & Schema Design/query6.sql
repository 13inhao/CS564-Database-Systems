SELECT COUNT(DISTINCT Item.SellerID)
FROM Item, Bid
WHERE Item.SellerID = Bid.UserID;