package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator, OpIterator {

    public static class Stat {
        public int v1;
        public int v2;

        Stat(int v1, int v2) {
            this.v1 = v1;
            this.v2 = v2;
        }
    }

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op aOp;

    private HashMap<Field, Stat> data = new HashMap<>();
    private Iterator<Field> keyIt;
    private TupleDesc tupleDesc;
    private Field defaultField = new IntField(0);

    /**
     * Aggregate constructor
     *
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
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
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Debug.log("merge tuple " + tup);
        Field key;
        if (gbfield != NO_GROUPING) {
            key = tup.getField(gbfield);
        } else {
            key = defaultField;
        }
        Stat oldStat;
        int INVALID = -1;
        int v;
        switch (aOp) {
            case MIN: // v1: min
                oldStat = data.getOrDefault(key, new Stat(Integer.MAX_VALUE, INVALID));
                v = ((IntField) tup.getField(afield)).getValue();
                data.put(key, new Stat(Integer.min(oldStat.v1, v), INVALID));
                break;
            case MAX: // v1: max
                oldStat = data.getOrDefault(key, new Stat(Integer.MIN_VALUE, INVALID));
                v = ((IntField) tup.getField(afield)).getValue();
                data.put(key, new Stat(Integer.max(oldStat.v1, v), INVALID));
                break;
            case SUM: // v1: sum
                oldStat = data.getOrDefault(key, new Stat(0, INVALID));
                v = ((IntField) tup.getField(afield)).getValue();
                data.put(key, new Stat(oldStat.v1 + v, INVALID));
                break;
            case AVG: // v1: sum v2: count
                oldStat = data.getOrDefault(key, new Stat(0, 0));
                v = ((IntField) tup.getField(afield)).getValue();
                data.put(key, new Stat(oldStat.v1 + v, oldStat.v2 + 1));
                break;
            case COUNT: // v1: count
                oldStat = data.getOrDefault(key, new Stat(0, INVALID));
//                v = ((IntField) tup.getField(afield)).getValue();
                data.put(key, new Stat(oldStat.v1 + 1, INVALID));
                break;
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return this;
//        throw new
//                UnsupportedOperationException("please implement me for lab2");
    }

    public void open()
            throws DbException, TransactionAbortedException {
        keyIt = data.keySet().iterator();
        Debug.log("hashmap is " + data.toString());
    }

    public boolean hasNext() throws DbException, TransactionAbortedException {
        return keyIt.hasNext();
    }

    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
//        if (!keyIt.hasNext()) {
//            return null;
//        }
        Field key = keyIt.next();
        Stat stat = data.get(key);
        Tuple t = new Tuple(tupleDesc);
        int v = 0;
        switch (aOp) {
            case MIN:
            case MAX:
            case SUM:
            case COUNT:
                v = stat.v1;
                break;
            case AVG:
                v = stat.v1 / stat.v2;
                break;
        }
        if (gbfield == NO_GROUPING) {
            t.setField(0, new IntField(v));
        } else {
            t.setField(0, key);
            t.setField(1, new IntField(v));
        }
        Debug.log("tuple is " + t.toString());
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
