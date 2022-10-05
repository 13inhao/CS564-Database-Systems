
"""
FILE: skeleton_parser.py
------------------

Author: Firas Abuzaid (fabuzaid@stanford.edu)
Author: Perth Charernwattanagul (puch@stanford.edu)
Modified: 04/21/2014

Skeleton parser for CS564 programming project 1. Has useful imports and
functions for parsing, including:

1) Directory handling -- the parser takes a list of eBay json files
and opens each file inside of a loop. You just need to fill in the rest.
2) Dollar value conversions -- the json files store dollar value amounts in
a string like $3,453.23 -- we provide a function to convert it to a string
like XXXXX.xx.
3) Date/time conversions -- the json files store dates/ times in the form
Mon-DD-YY HH:MM:SS -- we wrote a function (transformDttm) that converts to the
for YYYY-MM-DD HH:MM:SS, which will sort chronologically in SQL.

Your job is to implement the parseJson function, which is invoked on each file by
the main function. We create the initial Python dictionary object of items for
you; the rest is up to you!
Happy parsing!
"""


"""
CS564 2022Fall Project2 
ER Modeling & Schema Design

Author: Binhao Chen (bchen276@wisc.edu)
Author: Steven Yang (yang558@wisc.edu)
Author: Yishen Sun (yishen.sun@wisc.edu)
"""

import sys
from json import loads
from re import sub

columnSeparator = "|"

# global variable
Item_Array = []

categories_data = [] 
belong_data = []

bid_data = []
user_data = []

# Dictionary of months used for date transformation
MONTHS = {'Jan':'01','Feb':'02','Mar':'03','Apr':'04','May':'05','Jun':'06',\
        'Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}

"""
Returns true if a file ends in .json
"""
def isJson(f):
    return len(f) > 5 and f[-5:] == '.json'

"""
Converts month to a number, e.g. 'Dec' to '12'
"""
def transformMonth(mon):
    if mon in MONTHS:
        return MONTHS[mon]
    else:
        return mon

"""
Transforms a timestamp from Mon-DD-YY HH:MM:SS to YYYY-MM-DD HH:MM:SS
"""
def transformDttm(dttm):
    dttm = dttm.strip().split(' ')
    dt = dttm[0].split('-')
    date = '20' + dt[2] + '-'
    date += transformMonth(dt[0]) + '-' + dt[1]
    return date + ' ' + dttm[1]

"""
Transform a dollar value amount from a string like $3,453.23 to XXXXX.xx
"""

def transformDollar(money):
    if money == None or len(money) == 0:
        return money
    return sub(r'[^\d.]', '', money)

"""
Parses a single json file. Currently, there's a loop that iterates over each
item in the data set. Your job is to extend this functionality to create all
of the necessary SQL tables for your database.
"""
def parseJson(json_file):
    with open(json_file, 'r') as f:
        items = loads(f.read())['Items'] # creates a Python dictionary of Items for the supplied json file
        for item in items:
            itemtable(item)

            createCategoryTable(item)
            createBelongTable(item)

            createBidTable(item)
            createUserTable(item)



"""
Item table
"""
def itemtable(item):
    global Item_Array
    ItemID = str(item['ItemID'])
    Number_of_Bids = str(item['Number_of_Bids'])
    First_Bid = transformDollar(item['First_Bid'])
    # First_Bid = float(First_Bid)
    Currently = transformDollar(item['Currently'])
    # Currently = float(Currently)
    Buy_Price = transformDollar(item['Buy_Price']) if "Buy_Price" in item else 'NULL'
    # Buy_Price = float(Buy_Price)
    Name = item['Name'].replace('"','""')
    Description = item['Description'].replace('"','""') if item['Description'] is not None else 'NULL'
    Started = transformDttm(item['Started'])
    Ends = transformDttm(item['Ends'])
    SellerID = item['Seller']['UserID']
    Item_Array.append(ItemID + "|" + Number_of_Bids + "|" + First_Bid + "|" + Currently + "|\"" + '"|"'.join([Name, Buy_Price, Started, Ends, SellerID, Description]) + '"\n')
    


"""
Category table
"""
def createCategoryTable(item):
    global categories_data
    for category in item["Category"]:
        data = '\"' + sub(r'\"','\"\"', category) + '\"' + "\n"
        categories_data.append(data)
    categories_data = (list(dict.fromkeys(categories_data)))
    

"""
Belong table
"""
def createBelongTable(item):
    global belong_data
    itemID = str(item["ItemID"])
    categoryList = item["Category"]
    repeatedList = []
    for category in categoryList:
        if category not in repeatedList:
            repeatedList.append(category)
            data = itemID + "|" + '\"' + sub(r'\"','\"\"', category) + '\"' + "\n"
            belong_data.append(data)


"""
Bid table
"""
def createBidTable(item):
    global bid_data
    itemID = str(item['ItemID'])
    userID = ''
    time = ''
    amount = ''
    bidList = item['Bids']
    if bidList != None:
        for bid in bidList:
            if bid['Bid']['Bidder']['UserID'] != None:
                userID = sub(r'\"','\"\"',bid['Bid']["Bidder"]["UserID"])
            else:
                userID = "NULL"
            if bid['Bid']['Time'] != None:
                time = transformDttm(bid['Bid']['Time'])
            else:
                time = "NULL"
            if bid['Bid']['Amount'] != None:
                amount = transformDollar(bid['Bid']['Amount'])
            else:
                amount = "NULL"

            bid_data.append(f'{itemID}|"{userID}"|{time}|{amount}' + '\n')


"""
User table
"""
userID_list = []
def createUserTable(item):
    global user_data
    global userID_list
    # Get bidder
    userID = ''
    location = ''
    rating = ''
    country = ''

    bidList = item['Bids']
    if bidList != None:
        for bid in bidList:
            if bid['Bid']['Bidder']['UserID'] not in userID_list:
                userID_list.append(bid['Bid']['Bidder']['UserID'])
                userID = sub(r'\"','\"\"',bid['Bid']["Bidder"]["UserID"])
            else:
                continue

            if 'Location' in bid['Bid']['Bidder'].keys() and bid['Bid']['Bidder']['Location'] != None:
                location = sub(r'\"','\"\"',bid['Bid']['Bidder']['Location'])
            else:
                location = "NULL"

            if 'Country' in bid['Bid']['Bidder'].keys() and bid['Bid']['Bidder']['Country'] != None:
                country = sub(r'\"','\"\"',bid['Bid']['Bidder']['Country'])
            else:
                country = "NULL"

            if bid['Bid']['Bidder']['Rating'] != None:
                rating = bid['Bid']['Bidder']['Rating']
            else:
                rating = "NULL"
            
            user_data.append(f'"{userID}"|"{location}"|{rating}|"{country}"' + '\n')

    # Get seller
    seller = item["Seller"]
    sellerID = seller["UserID"]
    if sellerID not in userID_list:
        userID_list.append(sellerID)
        userID = sub(r'\"','\"\"',sellerID)
    
        if item['Location'] != None:
            location = sub(r'\"','\"\"',item['Location'])
        else:
            location = "NULL"

        if item['Country'] != None:
            country = sub(r'\"','\"\"',item['Country'])
        else:
            country = "NULL"

        if seller['Rating'] != None:
            rating = seller['Rating']
        else:
            rating = "NULL"

        user_data.append(f'"{userID}"|"{location}"|{rating}|"{country}"' + '\n')




"""
Loops through each json files provided on the command line and passes each file
to the parser
"""
def main(argv):
    if len(argv) < 2:
        print (sys.stderr, 'Usage: python skeleton_json_parser.py <path to json files>')
        sys.exit(1)
    # loops over all .json files in the argument
    for f in argv[1:]:
        if isJson(f):
            parseJson(f)
            print ("Success parsing " + f)
    # 
    with open("Item.dat","w") as f1: 
        f1.writelines(Item_Array)
    
    with open("Category.dat","w") as f2: 
        f2.writelines(categories_data)
    
    with open("Belong.dat","w") as f3: 
        f3.writelines(belong_data)

    with open("Bid.dat","w") as f4: 
        f4.writelines(bid_data)
    
    with open("User.dat","w") as f5: 
        f5.writelines(user_data)

if __name__ == '__main__':
    main(sys.argv)
