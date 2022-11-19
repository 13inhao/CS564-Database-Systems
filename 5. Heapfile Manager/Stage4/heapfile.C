#include "heapfile.h"
#include "error.h"

// routine to create a heapfile
const Status createHeapFile(const string fileName)
{
    File* 		file;
    Status 		status;
    FileHdrPage*	hdrPage;
    int			hdrPageNo;
    int			newPageNo;
    Page*		newPage;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status != OK)
    {
		// file doesn't exist. First create a new file.
        status = db.createFile(fileName);
		if (status != OK) {
            return status;
        };
		
        // open the new created file.
		status = db.openFile(fileName, file);
	    if (status != OK) {
            return status;
        };
		
		// allocate an empty header page and data page. 
        status = bufMgr->allocPage(file, hdrPageNo, newPage);
        if (status != OK) {
            return status;
        }

        // Take the Page* pointer returned from allocPage() cast it to a FileHdrPage*.
        hdrPage = (FileHdrPage*) newPage;
		
		// initialize the values in the header page. 
		strncpy(hdrPage->fileName, fileName.c_str(), MAXNAMESIZE); 
		hdrPage->firstPage = -1;
        hdrPage->lastPage = -1;
        hdrPage->pageCnt = 0;
        hdrPage->recCnt = 0;

        // make a second call to bm->allocPage(). 
        // This page will be the first data page of the file.
        status = bufMgr->allocPage(file, newPageNo, newPage);
        if (status != OK) {
            return status;
        }
        // invoke its init() method to initialize the page contents.
        newPage->init(newPageNo);

        // store the page number of the data page in
        // firstPage and lastPage attributes of the FileHdrPage
        hdrPage->firstPage = newPageNo;
        hdrPage->lastPage = newPageNo;
        hdrPage->pageCnt = 1;
        hdrPage->recCnt = 0;

        // unpin both pages and mark them as dirty.
        status = bufMgr->unPinPage(file, hdrPageNo, true);
        if (status != OK) {
            return status;
        }
        status = bufMgr->unPinPage(file, newPageNo, true);
        if (status != OK) {
            return status;
        }
        
        // after creating the empty heap file, close it before return;
        status = db.closeFile(file);
        if (status != OK) {
            return status;
        } 
        
        // All tasks are completed without any errors, return OK status.
        return OK;
    }
    return (FILEEXISTS);
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
	return (db.destroyFile (fileName));
}

// constructor opens the underlying file
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status 	status;
    Page*	pagePtr;

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
        // get the page number of the header page.
		status = filePtr->getFirstPage(headerPageNo);
		if (status != OK) {
			returnStatus = status;
            return;
		}

		// reads and pins the header page for the file in the buffer pool.
		status = bufMgr->readPage(filePtr, headerPageNo, pagePtr);
		if (status != OK) {
			returnStatus = status;
            return;
		}

        // initializing the private data members headerPage, and hdrDirtyFlag.
        // headerPageNo is initialized by calling the readPage function above;
		headerPage = (FileHdrPage*) pagePtr;
		hdrDirtyFlag = false;
		
		// read and pin the first page of the file into the buffer pool, 
        int firstPageNo = headerPage->firstPage;
        status = bufMgr->readPage(filePtr, firstPageNo, pagePtr);
        if (status != OK) {
			returnStatus = status;
            return;
		}

        // initializing the values of curPage, curPageNo, and curDirtyFlag appropriately. 
        curPage = pagePtr;
        curPageNo = firstPageNo;
        curDirtyFlag = false;
        
        // Set curRec to NULLRID.
        curRec = NULLRID;
		
        // All tasks are completed without any errors, return OK status.
        returnStatus = OK;
        return;
    }
    else
    {
    	cerr << "open of heap file failed\n";
		returnStatus = status;
		return;
    }
}

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it 
    if (curPage != NULL)
    {
    	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		curPage = NULL;
		curPageNo = 0;
		curDirtyFlag = false;
		if (status != OK) cerr << "error in unpin of date page\n";
    }
	
	 // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";
	
	// status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
	// if (status != OK) cerr << "error in flushFile call\n";
	// before close the file
	status = db.closeFile(filePtr);
    if (status != OK)
    {
		cerr << "error in closefile call\n";
		Error e;
		e.print (status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
  return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status;

    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;
   
    // Case 1: record is on the currently pinned page
    if (curPageNo == rid.pageNo) {
        // Get the record
        status = curPage->getRecord(rid, rec);
        curRec = rid;
        return status;
    } else { // Case 2: desired record is not on the currently pinned page
    
        // Unpin the currently pinned page
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK) {
            return status;
        }

        // Read the page into the buffer pool
        status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
        if (status != OK) {
            return status;
        }

        // Update the current pinned page number and record
        curPageNo = rid.pageNo;
        curRec = rid;
        curDirtyFlag = false;

        // get the record
        status = curPage->getRecord(rid, rec);
    }

    return status;
   
}

HeapFileScan::HeapFileScan(const string & name,
			   Status & status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
				     const int length_,
				     const Datatype type_, 
				     const char* filter_,
				     const Operator op_)
{
    if (!filter_) {                        // no filtering requested
        filter = NULL;
        return OK;
    }
    
    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int)
         || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}


const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
		curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo) 
    {
		if (curPage != NULL)
		{
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if (status != OK) return status;
		}
		// restore curPageNo and curRec values
		curPageNo = markedPageNo;
		curRec = markedRec;
		// then read the page
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;
		curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}


const Status HeapFileScan::scanNext(RID& outRid)
{
    Status 	status = OK;
    RID		nextRid;
    RID		tmpRid;
    int 	nextPageNo;
    Record      rec;
	
    // Case 1: no page is pinned in this file, check if the first record is satisfied.
    if (curPage == NULL) {

        // Start with the first page in the file
        curPageNo = headerPage->firstPage;

        // Check if the file is empty
        if (curPageNo < 0) {
            return FILEEOF;
        }

        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) {
            return status;
        }

        // Start with the first record in the first page
        status = curPage->firstRecord(tmpRid);
        if (status != OK) {
            return status;
        }

        // Set rid of record returned and the dirty bit
        curRec = tmpRid;
        curDirtyFlag = false;

        // Convert the rid to a pointer to the record
        status = curPage->getRecord(curRec, rec);
        if (status != OK) {
            return status;
        }

        // if the first record satisfies, return this record
        if (matchRec(rec) == true) {
            outRid = curRec;
            return OK;
        }

    }

    // Continue checking the rest of the records in the file (follow case 1),
    // Or, start with whatever page that has already pinned
    while(true) {
        // Get the next record
        status = curPage->nextRecord(curRec, nextRid);

        // curRec was the last record on the page
        if (status != OK) {
            while (status != OK) {

                // Get the next page in the file
                status = curPage->getNextPage(nextPageNo);
                if (status != OK) {
                    return status;
                }
                // if there is no next page
                if (nextPageNo < 0) {
                    return FILEEOF;
                }

                // unpin the curPage to get ready to read the next page
                status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
                if (status != OK) {
                    return status;
                }

                // read in the next page into the buffer pool
                curPageNo = nextPageNo;
                status = bufMgr->readPage(filePtr, curPageNo, curPage);
                if (status != OK) {
                    return status;
                }
                curDirtyFlag = false;

                // retrieve the first record of this page, and continue checking if this page is empty
                status = curPage->firstRecord(curRec);
            }
        } else {
            // nextRid is a valid and existing record
            curRec = nextRid;
        }

        // Convert the rid to a pointer to the record
        status = curPage->getRecord(curRec, rec);
        if (status != OK) {
            return status;
        }

        // if the first record satisfies, return this record
        if (matchRec(rec) == true) {
            outRid = curRec;
            return OK;
        }
    }

	return FILEEOF;
	
}


// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 

const Status HeapFileScan::getRecord(Record & rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file. 
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true; 
    return status;
}


// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length -1 ) >= rec.length)
	return false;

    float diff = 0;                       // < 0 if attr < fltr
    switch(type) {

    case INTEGER:
        int iattr, ifltr;                 // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr;               // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch(op) {
    case LT:  if (diff < 0.0) return true; break;
    case LTE: if (diff <= 0.0) return true; break;
    case EQ:  if (diff == 0.0) return true; break;
    case GTE: if (diff >= 0.0) return true; break;
    case GT:  if (diff > 0.0) return true; break;
    case NE:  if (diff != 0.0) return true; break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string & name,
                               Status & status) : HeapFile(name, status)
{
  //Do nothing. Heapfile constructor will bread the header page and the first
  // data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*	newPage;
    int		newPageNo;
    Status	status, unpinstatus;
    RID		rid;

    // check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }
    // if curPage is NULL or curPage is not the last page,
    // replace curPage with lastPage
    if(curPageNo != headerPage->lastPage || !curPage) {

      unpinstatus = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
      if(unpinstatus != OK) return unpinstatus;

      curPageNo = headerPage->lastPage;
      status = bufMgr->readPage(filePtr, curPageNo, curPage);
      if(status != OK) return status;
      curDirtyFlag = false;
    }

    // insert record and check whether current page is full
    status = curPage->insertRecord(rec, rid);
    if (status != OK && status != NOSPACE) return status;

    if(status == OK) {
      // bookkeeping
      curRec = rid;
      headerPage->recCnt++;
      curDirtyFlag = true;

      outRid = rid;
      return OK;
    }
    // nospace, allocate another page
    else {
      status = bufMgr->allocPage(filePtr, newPageNo, newPage);
      if(status != OK) return status;
      // new page init
      newPage->init(newPageNo);
      headerPage->lastPage = newPageNo;
      headerPage->pageCnt++;
      // link up the new page
      curPage->setNextPage(newPageNo);
      // write back
      unpinstatus = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
      if(unpinstatus != OK) return unpinstatus;
      // load the new page
      curPageNo = headerPage->lastPage;
      status = bufMgr->readPage(filePtr, curPageNo, curPage);
      if(status != OK) return status;

      status = curPage->insertRecord(rec, rid);
      if(status != OK) return status;
      // bookkeeping
      curRec = rid;
      headerPage->recCnt++;
      curDirtyFlag = true;
      // no next page
      curPage->setNextPage(-1);

      unpinstatus = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
      if(unpinstatus != OK) return unpinstatus;

      outRid = rid;
      return OK;
    }
  
}


