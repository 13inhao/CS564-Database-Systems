#include "catalog.h"
#include "query.h"

/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string &relation,
					   const int attrCnt,
					   const attrInfo attrList[])
{
	// part 6

	Status status = OK;

	// Get the detailed information of the attribute catalog of current relation
	AttrDesc *attrRel;
	int attrRelCnt;
	status = attrCat->getRelInfo(relation, attrRelCnt, attrRel);
	if (status != OK)
	{
		return status;
	}

	// Check attrCnt corresponds the relation attribute count
	if (attrRelCnt != attrCnt)
	{
		return ATTRTYPEMISMATCH;
	}

	// Compute the total size of a tuple in this relation
	int recordLength = 0;
	for (int i = 0; i < attrRelCnt; i++)
	{
		recordLength += attrRel[i].attrLen;
	}

	// Create an char array to store the tuple to be inserted (rearranged);
	char insertTuple[recordLength];

	// Iterate through both the attrList[] and the attrRel
	for (int i = 0; i < attrCnt; i++)
	{
		// Before write this arribute, check if it is a NULL:
		if (attrList[i].attrValue == NULL)
		{
			// Reject the insertion as Minirel does not implement NULLs
			return ATTRTYPEMISMATCH;
		}
		for (int j = 0; j < attrRelCnt; j++)
		{
			//  Check if the attibute name match:
			if (strcmp(attrRel[j].attrName, attrList[i].attrName) == 0)
			{
				// check the type of  attrList[i] and convert it to a proper one.
				int tmpInt;
				float tmpFloat;
				switch (attrList[i].attrType)
				{
				case INTEGER:
					tmpInt = atoi((char *)attrList[i].attrValue);
					memcpy(insertTuple + attrRel[j].attrOffset, &tmpInt, attrRel[j].attrLen);
					break;
				case FLOAT:
					tmpFloat = atof((char *)attrList[i].attrValue);
					memcpy(insertTuple + attrRel[j].attrOffset, &tmpFloat, attrRel[j].attrLen);
					break;
				case STRING:
					memcpy(insertTuple + attrRel[j].attrOffset, attrList[i].attrValue, attrRel[j].attrLen);
					break;
				}
			}
		}
	}

	// Create a record instance for inserting the new tuple
	Record new_rec;
	new_rec.data = (void *)insertTuple;
	new_rec.length = recordLength;

	// Instantiate an InsertFileScan object for the ready insertion
	InsertFileScan isf(relation, status);
	if (status != OK)
	{
		return status;
	}

	// Use InsertFileScan::insertRecord() to insert the new tuple
	RID rid;
	status = isf.insertRecord(new_rec, rid);
	if (status != OK)
	{
		return status;
	}

	return OK;
}
