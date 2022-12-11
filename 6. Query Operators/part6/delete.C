#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
	// part 6

	// Initialize variables
	Status status;
	AttrDesc descriptor;

	// Initialize a HeapFileScan
	HeapFileScan scanner(relation, status);
	if (status != OK) {
		return status;
	}
	
	// Step 1: Set up an appropriate startScan with correct parameters

	// Special case: when attrName is null
	// Set startScan offset = 0, length = 0, type = STRING, filter = NULL
	if (attrName.empty()) {
		status = scanner.startScan(0, 0, STRING, NULL, op);
		if (status != OK) {
			return status;
		}
	} else {
		// Get the attribute descriptor record for attribute attrName in relation relName
		status = attrCat->getInfo(relation, attrName, descriptor);
		if (status != OK) {
			return status;
		}
		
		// Cast the attrValue(filter) into the correct Datatype
		const char *filter;
		int i;
		float f;
		switch (type) {
			case FLOAT:
				{
					f = atof(attrValue);
					filter = (char *) &f;
					break;
				}
			case INTEGER:
				{
					i = atoi(attrValue);
					filter = (char *) &i;
					break;
				}
			case STRING:
				filter = attrValue;
				break;
		}	

		// Get the offset and length of the attribute from the descriptor
		int offset = descriptor.attrOffset;
		int length = descriptor.attrLen;
		status = scanner.startScan(offset, length, type, filter, op);
		if (status != OK) {
			return status;
		}
	}

	// Start the scan with the predicates
	RID recordID;
	while (scanner.scanNext(recordID) == OK) {
		// Delete the current matching record
		status = scanner.deleteRecord();
		if (status != OK) {
			return status;
		}
	}
	
    return OK;

}


