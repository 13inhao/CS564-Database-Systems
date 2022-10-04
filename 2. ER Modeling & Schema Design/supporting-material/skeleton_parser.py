
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

import sys
from json import loads
from re import sub

columnSeparator = "|"

# global variable
Item_Array = []

categories_data = [] 
belong_data = []

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
    Description = item['Description'].replace('"','""') if item['Description'] is not None else ""
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
    for category in categoryList:
        data = itemID + "|" + '\"' + sub(r'\"','\"\"', category) + '\"' + "\n"
        belong_data.append(data)


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
    
    with open("Categories.dat","w") as f2: 
        f2.writelines(categories_data)
    
    with open("belong.dat","w") as f3: 
        f3.writelines(belong_data)

if __name__ == '__main__':
    main(sys.argv)
