package simpledb;

import sun.jvm.hotspot.oops.FieldType;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private ArrayList<TDItem> columns = new ArrayList<TDItem>();
    private int byteLen = 0;
    private HashMap<String, Integer> name2IdxMap = new HashMap<String, Integer>();

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return columns.iterator();
//        return null;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        for (int i=0;i<typeAr.length;i++){
            String field = null;
            if (fieldAr!=null && i<fieldAr.length){
                field = fieldAr[i];
            }
            TDItem item = new TDItem(typeAr[i], field);
            columns.add(item);
            if (field!=null){
                name2IdxMap.put(field, i);
            }
            byteLen += typeAr[i].getLen();
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this(typeAr, null);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return columns.size();
//        return 0;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i>=columns.size()){
            throw new NoSuchElementException();
        }
        return columns.get(i).fieldName;
//        return null;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i>=columns.size()){
            throw new NoSuchElementException();
        }
        return columns.get(i).fieldType;
//        return null;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if (name!=null && name2IdxMap.containsKey(name)){
            return name2IdxMap.get(name);
        }
        throw new NoSuchElementException();
//        return 0;
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        return byteLen;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        ArrayList<Type> fieldtypes = new ArrayList<Type>();
        ArrayList<String> fieldnames = new ArrayList<String>();
        for (TDItem col :td1.columns){
            fieldtypes.add(col.fieldType);
            fieldnames.add(col.fieldName);
        }
        for (TDItem col :td2.columns){
            fieldtypes.add(col.fieldType);
            fieldnames.add(col.fieldName);
        }
        return new TupleDesc(fieldtypes.toArray(new Type[0]), fieldnames.toArray(new String[0]));
//        return null;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if (!(o instanceof TupleDesc)){
            return false;
        }
        TupleDesc td2 = (TupleDesc) o;
        if (this.columns.size()!=td2.columns.size()){
            return false;
        }
        for (int i=0;i<this.columns.size();i++){
            if (this.columns.get(i).fieldType!=td2.columns.get(i).fieldType){
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        /*

         */
        int acc = 0;
        for (int i=0;i<columns.size();i++){
            switch (columns.get(i).fieldType){
                case INT_TYPE:
                    acc += 17*i*30;
                    break;
                case STRING_TYPE:
                    acc += 101*i*30;
                    break;
            }
        }
        return acc;
//        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder buf = new StringBuilder();
        for (int i=0;i<columns.size();i++){
            buf.append(columns.get(i).fieldType).append("(")
                    .append(columns.get(i).fieldName).append(")");
            if (i!=columns.size()-1){
                buf.append(",");
            }
        }
        return buf.toString();
    }
}
