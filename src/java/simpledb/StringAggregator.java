package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator, OpIterator {

    private static final long serialVersionUID = 1L;

    public static class Stat {
        public int v1;
        public int v2;

        Stat(int v1, int v2) {
            this.v1 = v1;
            this.v2 = v2;
        }
    }

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op aOp;

    private HashMap<Field, IntegerAggregator.Stat> data = new HashMap<>();
    private Iterator<Field> keyIt;
    private TupleDesc tupleDesc;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.aOp = what;
        if (gbfield == NO_GROUPING) {
            tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field key = tup.getField(gbfield);
        IntegerAggregator.Stat oldStat;
        int INVALID = -1;
        int v;
        switch (aOp) {
            case COUNT: // v1: count
                oldStat = data.getOrDefault(key, new IntegerAggregator.Stat(0, INVALID));
                data.put(key, new IntegerAggregator.Stat(oldStat.v1 + 1, INVALID));
                break;
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return this;
//        throw new UnsupportedOperationException("please implement me for lab2");
    }

    public void open()
            throws DbException, TransactionAbortedException {
        keyIt = data.keySet().iterator();
    }

    public boolean hasNext() throws DbException, TransactionAbortedException {
        return keyIt.hasNext();
    }

    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
//        if (!keyIt.hasNext()) {
//            return null;
//        }
        Field key = keyIt.next();
        IntegerAggregator.Stat stat = data.get(key);
        Tuple t = new Tuple(tupleDesc);
        int v = 0;
        switch (aOp) {
            case COUNT:
                v = stat.v1;
                break;
        }
        if (gbfield == NO_GROUPING) {
            t.setField(0, new IntField(v));
        } else {
            t.setField(0, key);
            t.setField(1, new IntField(v));
        }
        return t;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        keyIt = data.keySet().iterator();
    }

    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    public void close() {
        keyIt = null;
    }

}
