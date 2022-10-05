DROP TABLE if exists Category;
DROP TABLE if exists Belong;
DROP TABLE if exists User;
DROP TABLE if exists Item;
DROP TABLE if exists Bid;

CREATE TABLE Category (
    CategoryID      VARCHAR(255)    PRIMARY KEY
);

CREATE TABLE User (
    UserID          VARCHAR(255)    PRIMARY KEY,
    Location        VARCHAR(255),
    Rating          INTEGER         NOT NULL,
    Country         VARCHAR(255)
);

CREATE TABLE Item (
    ItemID          INTEGER         PRIMARY KEY,
    Number_of_bids  INTEGER         NOT NULL,
    First_Bid       DOUBLE          NOT NULL,
    Currently       DOUBLE          NOT NULL,
    Name            VARCHAR(255)    NOT NULL,
    Buy_Price       DOUBLE,
    Started         DATETIME        NOT NULL,
    Ends            DATETIME        NOT NULL,
    SellerID        VARCHAR(255),
    Description     VARCHAR(255),
    FOREIGN KEY (SellerID) REFERENCES User (UserID)
);

CREATE TABLE Belong (
    ItemID          INTEGER,
    CategoryID      VARCHAR(255),
    PRIMARY KEY (CategoryID, ItemID),
    FOREIGN KEY (CategoryID) REFERENCES Category (CategoryID),
    FOREIGN KEY (ItemID) REFERENCES Item (ItemID)
);

CREATE TABLE Bid (
    ItemID      INTEGER             NOT NULL,
    UserID      VARCHAR(255)        NOT NULL,
    Time        DATETIME            NOT NULL,
    Amount      DOUBLE              NOT NULL,
    FOREIGN KEY (UserID) REFERENCES User (UserID),
    FOREIGN KEY (ItemID) REFERENCES Item (ItemID),
    PRIMARY KEY (UserID, ItemID, Time)
);
