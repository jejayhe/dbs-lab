package simpledb;

import java.util.ArrayList;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;
    private int min;
    private int max;
    private int count = 0;
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
        } else if (max - min < buckets) {
            this.buckets = max - min;
        }
        bounds = new int[this.buckets][3];
        for (int i = 0; i < this.buckets; i++) {
            int[] tuple = bucketBound(i);
            bounds[i][0] = tuple[0];
            bounds[i][1] = tuple[1];
            bounds[i][2] = 0;
            Debug.log("bounds[" + i + "] = " + tuple[0] + " " + tuple[1]);
        }
    }

    // return leftbound, rightbound for bucket i
    public int[] bucketBound(int i) {
        int leftbound = min + (max - min) * i / buckets;
        int rightbound = 0;
        if (i == buckets - 1) {
            rightbound = max;
        } else {
            if (((max - min) * (i + 1)) % buckets == 0) {
                rightbound = min + (max - min) * (i + 1) / buckets - 1;
            } else {
                rightbound = min + (max - min) * (i + 1) / buckets;
            }
        }
        return new int[]{leftbound, rightbound};
    }

    private int[][] bounds;

    // return bucket index that value v resides in
    // return -1 if too left
    // return this.buckets if too right
    public int searchBucket(int v) {
        if (v < min) {
            return -1;
        }
        if (v > max) {
            return this.buckets;
        }
        // find first value that is less than v
        int i = 0;
        int j = this.buckets - 1;
//        int mid = (i+j)/2;
        while (i < j) {
            int mid = (i + j) / 2;
            if (bounds[mid][0] > v) {
                j = mid - 1;
            } else if (bounds[mid][1] < v) {
                i = mid + 1;
            } else {
                return mid;
            }
        }
        return i;
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
        count++;
        bounds[searchBucket(v)][2]++;
    }

    public double computeGreaterThanSelectivity(int v) {
        if (v < min) {
            return 1;
        }
        if (v >= max) {
            return 0;
        }
        // now that min <= v < max
        double acc = 0;
        // if v is right bound, sum others
        int vBucketIndex = searchBucket(v);
        if (bounds[vBucketIndex][1] == v) {
            for (int i = vBucketIndex + 1; i < buckets; i++) {
                acc += bounds[i][2];
            }
        } else {
            int leftb = bounds[vBucketIndex][0];
            int rightb = bounds[vBucketIndex][1];
            acc += (rightb - v) * 1.0 / (rightb - leftb + 1) * bounds[vBucketIndex][2];
            for (int i = vBucketIndex + 1; i < buckets; i++) {
                acc += bounds[i][2];
            }
        }
        return acc / count;
    }

    public double computeLessThanSelectivity(int v) {
        if (v <= min) {
            return 0;
        }
        if (v > max) {
            return 1;
        }
        // now that min < v <= max
        double acc = 0;
        // if v is left bound, sum others
        int vBucketIndex = searchBucket(v);
        if (bounds[vBucketIndex][0] == v) {
            for (int i = vBucketIndex - 1; i >= 0; i--) {
                acc += bounds[i][2];
            }
        } else {
            int leftb = bounds[vBucketIndex][0];
            int rightb = bounds[vBucketIndex][1];
            acc += 1.0 * (v - leftb) / (rightb - leftb + 1) * bounds[vBucketIndex][2];
            for (int i = vBucketIndex - 1; i >= 0; i--) {
                acc += bounds[i][2];
            }
        }
        return acc / count;
    }

    public double computeEqualSelectivity(int v) {
        if (v < min) {
            return 0;
        }
        if (v > max) {
            return 0;
        }
        int vBucketIndex = searchBucket(v);
        int leftb = bounds[vBucketIndex][0];
        int rightb = bounds[vBucketIndex][1];
        return 1.0 * bounds[vBucketIndex][2] / (rightb - leftb + 1) / count;
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
                if (max == min) {
                    if (v == min) {
                        return 1;
                    } else {
                        return 0;
                    }
                } else {
                    res = computeEqualSelectivity(v);
                }
                break;
            case GREATER_THAN:
                if (max == min) {
                    if (v < min) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
                res = computeGreaterThanSelectivity(v);
                break;
            case GREATER_THAN_OR_EQ:
                if (max == min) {
                    if (v <= min) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
                res = computeGreaterThanSelectivity(v - 1);
                break;
            case LESS_THAN:
                if (max == min) {
                    if (v > min) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
                res = computeLessThanSelectivity(v);
                break;
            case LESS_THAN_OR_EQ:
                if (max == min) {
                    if (v >= min) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
                res = computeLessThanSelectivity(v + 1);
                break;
            case LIKE:
                res = 1;
                break;
            case NOT_EQUALS:
                if (max == min) {
                    if (v == min) {
                        res = 0;
                    } else {
                        res = 1;
                    }
                } else {
                    res = 1 - computeEqualSelectivity(v);
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

    public void print() {
        for (int i = 0; i < buckets; i++) {
            Debug.log("bounds[" + i + "]: " + bounds[i][0] + " " + bounds[i][1] + " " + bounds[i][2]);
        }
    }
}
