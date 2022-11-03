#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}


const Status BufMgr::allocBuf(int & frame) {
    Status status;
    // count pinned frames to handle BUFFEREXCEEDED
    int count_pinned_frames = 0;
    // numBufs is total number of frames
    while (count_pinned_frames < numBufs) {
        // increase clockHand
        advanceClock();
        // get buf desc ptr
        BufDesc *bptr = &bufTable[clockHand];
        // check validation
        if (!bptr->valid) {
            bptr->Clear();
            frame = bptr->frameNo;
            return OK;
        }
        // check refbit
        if (bptr->refbit) {
            bptr->refbit = false;
            continue;
        }
        // check pinned
        if (bptr->pinCnt > 0) {
            count_pinned_frames++;
            continue;
        }
        // check dirty bit
        if (bptr->dirty) {
            // flush page to disk
            Page *pptr = &bufPool[clockHand];
            status = bptr->file->writePage(bptr->pageNo, pptr);
            if (status != OK) {
                return status;
            }
        }
        // remove mapping (File, page) -> frame from hashTable
        status = hashTable->remove(bptr->file, bptr->pageNo);
        if (status != OK) {
            return status;
        }
        //
        bptr->Clear();
        frame = bptr->frameNo;
        return OK;
    }
    // all page are pinned
    return BUFFEREXCEEDED;
}

	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page) {
    int frameNo;

    // Check if page is already in the buffer pool
    Status isInPool =  hashTable->lookup(file, PageNo, frameNo);

    // Case 1: Page is not in the buffer pool
    if (isInPool == HASHNOTFOUND) {
        // Allocate a buffer frame
        Status alloc_frame = allocBuf(frameNo);
        if (alloc_frame != OK) {
            return alloc_frame;
        }

        // Read the page from disk into the pool frame
        Page *fpptr = &bufPool[frameNo];
        Status read_page = file->readPage(PageNo, fpptr);
        if (read_page != OK) {
            return read_page;
        }

        // Insert the page into the hashtable
        Status insert_page = hashTable->insert(file, PageNo, frameNo);
        if (insert_page != OK) {
            return insert_page;
        }

        // Invoke Set() on the frame
        bufTable[frameNo].Set(file, PageNo);

        // return page pointer to the frame via parameter
        page = &bufPool[frameNo];


    } else { // Case 2: Page is in the buffer pool
        // Set refbit to true
        bufTable[frameNo].refbit = true;

        // Increment pinCnt
        bufTable[frameNo].pinCnt += 1;

        // return page pointer to the frame via parameter
        page = &bufPool[frameNo];

    }

    return OK;

}


/**
 * Decrements the pinCnt of the frame containing (file, PageNo) and, 
 * if dirty == true, sets the dirty bit.  
 * 
 * 
 * Returns OK if no errors occurred, 
 * HASHNOTFOUND if the page is not in the buffer pool hash table, 
 * PAGENOTPINNED if the pin count is already 0.
 * 
 */
const Status BufMgr::unPinPage(File *file, const int PageNo, const bool dirty) {
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, PageNo, frameNo);

    // if it is not in the buffer pool, directly return HASHNOTFOUND
    if(status != OK){
        return status;
    }

    // Decrements the pinCnt of the frame containing,
    // return PAGENOTPINNED if the pin count is already 0.
    if(bufTable[frameNo].pinCnt == 0) {
        return PAGENOTPINNED;
    }
    bufTable[frameNo].pinCnt--;

    // if dirty == true, sets the dirty bit
    if(dirty) {
        bufTable[frameNo].dirty = true;
    }
    
    // Returns OK if no errors occurred
    return OK;

}

/**
 * Allocates a buffer pool frame (page) for the current file 
 * and returns the page number of the newly allocated page via the pageNo parameter 
 * and a pointer to the buffer frame allocated for the page via the page parameter. 
 * 
 * Returns OK if no errors occurred, 
 * UNIXERR if a Unix error occurred, 
 * BUFFEREXCEEDED if all buffer frames are pinned 
 * and HASHTBLERROR if a hash table error occurred. 
 * 
 */
const Status BufMgr::allocPage(File *file, int &pageNo, Page *&page) {
    int newPageNumber = 0;
    Status file_alloc_status = file->allocatePage(newPageNumber);

    // Return corresponding error if we fails to 
    // allocate an empty page by calling allocatePage().
    if(file_alloc_status != OK){
        return file_alloc_status;
    }

    // allocBuf() is called to obtain a buffer pool frame
    int newFrame = 0;
    Status frame_alloc_status = allocBuf(newFrame);
    if(frame_alloc_status != OK) {
        return frame_alloc_status;
    }

    Status insert_status = hashTable->insert(file, newPageNumber, newFrame);
    if(insert_status != OK) {
        return insert_status;
    }

    BufDesc* bufPtr = &bufTable[newFrame];
    bufPtr->Set(file, newPageNumber);

    pageNo = newPageNumber;

    Page* pagePtr = &bufPool[newFrame];
    page = pagePtr;
    
    return OK;

}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


