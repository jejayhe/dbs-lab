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
public class HeapFile implements DbFile, DbFileIterator {
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
        // some code goes here
        int pgNo = pid.getPageNumber();
        try {
            DataInputStream in = new DataInputStream(new FileInputStream(file));
            int pgSize = Database.getBufferPool().getPageSize();
            byte[] data = new byte[pgSize];
            in.skipBytes(pgSize * pgNo);
            in.read(data, 0, pgSize);
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
//        int pgSize = Database.getBufferPool().getPageSize();
//        return (int) file.length() / pgSize;
        return numPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return this;
//        return null;
    }

    private int curPgNo;
    //    private int nextTupleNo;
    private HeapPage curPage;
    private HeapPageId curPageId;
    private Iterator<Tuple> curIt;

    public void open() throws DbException, TransactionAbortedException {
        curPageId = new HeapPageId(this.getId(), curPgNo);
        Page page = Database.getBufferPool().getPage(null, curPageId, null);
        try {
            curPage = new HeapPage(curPageId, page.getPageData());
            curIt = curPage.iterator();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (curIt == null) {
            return false;
        }
        return curIt.hasNext() || curPgNo < numPages - 1;
    }

    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (curIt == null) {
            throw new NoSuchElementException();
        }
        if (curIt.hasNext()) {
            return curIt.next();
        } else {
            curPgNo++;
            curPageId = new HeapPageId(this.getId(), curPgNo);
            Page page = Database.getBufferPool().getPage(null, curPageId, null);
            try {
                curPage = new HeapPage(curPageId, page.getPageData());
                curIt = curPage.iterator();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return next();
        }
//        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        curPgNo = 0;
        open();
    }

    public void close() {
        curPgNo = 0;
        curPage = null;
        curPageId = null;
        curIt = null;
    }
}

