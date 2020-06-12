package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile extends AbstractDbFileIterator implements DbFile, DbFileIterator {
    private int heapFileId;
    private File file;
    private TupleDesc tupleDesc;
    private int numPages;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        heapFileId = f.getAbsoluteFile().hashCode();
        file = f;
        tupleDesc = td;
        int pgSize = Database.getBufferPool().getPageSize();
        numPages = (int) file.length() / pgSize;
//        Debug.log("when creating heapfile, numpages is :" + numPages);
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return heapFileId;
//        throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
//        throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pgNo = pid.getPageNumber();
//        if (pgNo == numPages) {
//            Page newp = new HeapPage(new HeapPageId(heapFileId, pgNo), tupleDesc);
////            Debug.log("numpages current " + numPages + " instantly updating");
//            numPages++;
//            return newp;
//        }
        // some code goes here
//        Debug.log("reading pgNo :" + pgNo);
        try {
            DataInputStream in = new DataInputStream(new FileInputStream(file));
            int pgSize = Database.getBufferPool().getPageSize();
            byte[] data = new byte[pgSize];
            in.skipBytes(pgSize * pgNo);
            in.read(data, 0, pgSize);
//            Debug.log("data[0] = " + data[0]);
            in.close();
            return new HeapPage(new HeapPageId(pid.getTableId(), pgNo), data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int pgSize = Database.getBufferPool().getPageSize();
        numPages = (int) file.length() / pgSize;
        return numPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        for (int pgNo = 0; pgNo < numPages; pgNo++) {
            PageId pid = new HeapPageId(this.getId(), pgNo);
            HeapPage hpage = (HeapPage) Database.getBufferPool().getPage(null, pid, null);
            try {
                hpage.insertTuple(t);
                return new ArrayList<Page>() {{
                    add(hpage);
                }};
            } catch (DbException e) {
            }
        }
        Debug.log("pages are full; create a blank page");
        int pgNo = numPages;
        BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(getFile(), true));
        byte[] emptyData = HeapPage.createEmptyPageData();
        bw.write(emptyData);
        bw.close();
        HeapPageId newPid = new HeapPageId(heapFileId, pgNo);
        numPages();
        HeapPage hpage = (HeapPage) Database.getBufferPool().getPage(null, newPid, null);
        try {
            hpage.insertTuple(t);
            return new ArrayList<Page>() {{
                add(hpage);
            }};
        } catch (DbException e) {
            Debug.log("new page insert fail");
            e.printStackTrace();
        }
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        PageId pid = t.getRecordId().getPageId();
        HeapPage hpage = (HeapPage) Database.getBufferPool().getPage(null, pid, null);
        hpage.deleteTuple(t);
        return new ArrayList<Page>() {{
            add(hpage);
        }};
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return this;
//        return null;
    }

    private int curPgNo = 0;
    //    private int nextTupleNo;
    private HeapPage curPage;
    private HeapPageId curPageId;
    private Iterator<Tuple> curIt;

    public void open() throws DbException, TransactionAbortedException {
        curPageId = new HeapPageId(this.getId(), curPgNo);
        curPage = (HeapPage) Database.getBufferPool().getPage(null, curPageId, null);
        curIt = curPage.iterator();
    }

    public Tuple readNext() throws DbException, TransactionAbortedException {
        if (curIt == null) {
            throw new NoSuchElementException();
        }
        if (curIt.hasNext()) {
            Debug.log("calling curIt.next()");
            return curIt.next();
        } else {
            Debug.log("curPgNo ++ ");
            curPgNo++;
            if (curPgNo >= numPages) {
                return null;
            }
            curPageId = new HeapPageId(this.getId(), curPgNo);
            curPage = (HeapPage) Database.getBufferPool().getPage(null, curPageId, null);
            curIt = curPage.iterator();
            return readNext();
        }
    }

    public void rewind() throws DbException, TransactionAbortedException {
        curPgNo = 0;
        open();
        super.close();
    }

    public void close() {
        curPgNo = 0;
        curPage = null;
        curPageId = null;
        curIt = null;
        super.close();
    }
}

