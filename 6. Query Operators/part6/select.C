#include "catalog.h"
#include "query.h"


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
    // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;

	// declare variables
	Status status = OK;
    int length = 0;
    AttrDesc attrDesc;
    AttrDesc* projAttrInfo;
    char* filter;
    int tmp_i;
    float tmp_f;

    projAttrInfo = new AttrDesc[projCnt];

    // get the description of attributes in a loop
    for (int i = 0; i < projCnt; i++) {
        status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, projAttrInfo[i]); 
        if (status != OK) return status;
        length += projAttrInfo[i].attrLen;
    }
    
    if (attr == NULL) {
        status = ScanSelect(result, projCnt, projAttrInfo, NULL, op, NULL, length);
    } else {
        status = attrCat->getInfo(attr->relName, attr->attrName, attrDesc);
        if(status != OK) return status;
        
        // create the filter according to the attribute type
        if (attrDesc.attrType == INTEGER) {
            filter = (char *)malloc(sizeof(int));
            tmp_i = atoi(attrValue);
            memcpy(filter, (char*)&tmp_i, sizeof(int));
        
        } else if (attrDesc.attrType == FLOAT) {
            filter = (char *)malloc(sizeof(float));
            tmp_f = atof(attrValue);
            memcpy(filter, (char*)&tmp_f, sizeof(float));
            
        } else {
            filter = (char *)malloc(strlen(attrValue) + 1);
            memcpy(filter, attrValue, strlen(attrValue) + 1);
        }

        status = ScanSelect(result, projCnt, projAttrInfo, &attrDesc, op, filter, length);
        free(filter);
    }
    if (status != OK) return status;
    // delete pointer
    delete projAttrInfo;
    return status;
}


const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

 	Status status = OK;
    Record outputRec;
    RID rid;
    char* outputData;
    InsertFileScan *ifs;
    HeapFileScan *hfs;

    //init InsertFileScan object
    ifs = new InsertFileScan(result, status);
    if (status != OK) return status; 

    //init HeapFileScan object
    hfs = new HeapFileScan(projNames->relName, status);
    if (status != OK) return status; 

    // init outputData memory
    outputData = (char *)malloc(reclen);
    if (!outputData) return INSUFMEM;
    
    outputRec.data = outputData;
    outputRec.length = reclen;
    
    // check filter exists and apply filter
    if (filter != NULL) {
        status = hfs->startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype)attrDesc->attrType, filter, op);
    } else {
        status = hfs->startScan(0, 0, STRING, NULL, op);
    }
    if (status != OK) return status; 
    
    // copy records repeatedly
    while((status = hfs->scanNext(rid)) == OK) {
        Record tmpRec;
        status = hfs->getRecord(tmpRec);
        if (status != OK) return status;
        
        int offset = 0;
        for (int i = 0; i < projCnt; i++) {
            memcpy(outputData + offset, (char *)tmpRec.data + projNames[i].attrOffset, projNames[i].attrLen);
            offset += projNames[i].attrLen;
        }
        
        status = ifs->insertRecord(outputRec, rid);
        if (status != OK) return status;
    }
    // delete pointers
    if (status == FILEEOF) status = OK;
    delete ifs;
    delete hfs;
    return status;
}
