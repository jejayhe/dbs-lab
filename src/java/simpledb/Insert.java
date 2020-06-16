package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

import static simpledb.Type.INT_TYPE;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator child;
    private int tableId;
    private TupleDesc tupleDesc;
    private boolean isReturned;
    private TransactionId tid;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
        this.tupleDesc = new TupleDesc(new Type[]{INT_TYPE});
        this.isReturned = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        child.open();
        isReturned = false;
    }

    public void close() {
        // some code goes here
        child.close();
        isReturned = true;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        isReturned = false;
    }

    public boolean hasNext() throws DbException, TransactionAbortedException {
        return !isReturned;
    }

    public Tuple next() throws DbException, TransactionAbortedException,
            NoSuchElementException {
        return fetchNext();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (isReturned) {
            return null;
        }
        int acc = 0;
        while (child.hasNext()) {
            Tuple t = child.next();
            try {
                Database.getBufferPool().insertTuple(tid, tableId, t);
                acc++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Tuple res = new Tuple(new TupleDesc(new Type[]{INT_TYPE}));
        res.setField(0, new IntField(acc));
        isReturned = true;
        return res;

//        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
//        return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if (children != null && children.length == 1) {
            child = children[0];
        }
    }
}
