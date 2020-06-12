package simpledb;

import java.util.ArrayList;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;
    private double min;
    private double max;
    private int total = 0;
    double width;
    ArrayList<Integer> data;

    /**
     * Create a new IntHistogram.
     *
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     *
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     *
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        this.buckets = buckets;
        this.max = max;
        this.min = min;
        data = new ArrayList<>();
        for (int i = 0; i < buckets; i++) {
            data.add(0);
        }
        if (max == min) {
            this.buckets = 1;
            width = 1;
        } else {
            width = (max - min) * 1.0 / this.buckets;
        }

    }

    public int computeBucket(double v) {
        if (this.max - this.min == 0) {
            return 0;
        }
        int bucketNo = (int) (Math.floor((v - this.min) * this.buckets / (this.max - this.min)));
        if (bucketNo >= this.buckets) {
            bucketNo = this.buckets - 1;
        }
        if (bucketNo < 0) {
            bucketNo = 0;
        }
        return bucketNo;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     *          [min, min+width)
     *          [min+width, min+2*width)
     *          ...
     *          [max-width, max]
     */

    public void addValue(int v) {
        // some code goes here
        total++;
        int bucketNo = computeBucket(v);
        data.set(bucketNo, data.get(bucketNo) + 1);
    }

    public double computeGreaterThanSelectivity(double v) {
        int bucketNo = computeBucket(v);
        double leftBound = this.min + this.width * bucketNo;
        double rightBound = this.min + this.width * (bucketNo + 1);
        double w = rightBound - v;
        if (w < 0) {
            w = 0;
        }
        if (w > this.width) {
            w = this.width;
        }
        double res = w / width * this.data.get(bucketNo);
        for (int i = bucketNo + 1; i < this.buckets; i++) {
            res += this.data.get(i);
        }
        return res / total;
    }

    public double computeLessThanSelectivity(double v) {
        int bucketNo = computeBucket(v);
        double leftBound = this.min + this.width * bucketNo;
        double rightBound = this.min + this.width * (bucketNo + 1);
        double w = v - leftBound;
        if (w < 0) {
            w = 0;
        }
        if (w > this.width) {
            w = this.width;
        }
        double res = w / width * this.data.get(bucketNo);
        for (int i = bucketNo - 1; i >= 0; i--) {
            res += this.data.get(i);
        }
        return res / total;
    }


    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     *
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        double res = 0;
        switch (op) {
            case EQUALS:
                if (width == 0) {
                    if (v == this.min) {
                        res = 1;
                    } else {
                        res = 0;
                    }
                } else {
                    res = data.get(computeBucket(v)) / width / total;
                }
                break;
            case GREATER_THAN:
                res = computeGreaterThanSelectivity(v);
                break;
            case GREATER_THAN_OR_EQ:
                res = computeGreaterThanSelectivity(v - 0.5);
                break;
            case LESS_THAN:
                res = computeLessThanSelectivity(v);
                break;
            case LESS_THAN_OR_EQ:
                res = computeLessThanSelectivity(v + 0.5);
                break;
            case LIKE:
                res = 1;
                break;
            case NOT_EQUALS:
                if (width == 0) {
                    if (v == this.min) {
                        res = 0;
                    } else {
                        res = 1;
                    }
                } else {
                    res = 1 - data.get(computeBucket(v)) / width / total;
                }
                break;
        }
        // some code goes here
        return res;
    }

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
