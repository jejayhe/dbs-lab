package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

import static simpledb.Type.INT_TYPE;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator child;
    private TupleDesc tupleDesc;
    private boolean isReturned;
    private TransactionId tid;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.child = child;
        this.tupleDesc = new TupleDesc(new Type[]{INT_TYPE});
        this.isReturned = false;
        this.tid = t;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
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
                Database.getBufferPool().deleteTuple(tid, t);
                acc++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Tuple res = new Tuple(new TupleDesc(new Type[]{INT_TYPE}));
        res.setField(0, new IntField(acc));
        isReturned = true;
        return res;
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
