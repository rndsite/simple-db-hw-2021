package simpledb.optimizer;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private final int buckets;
    private final int min;
    private final int max;
    private final int width;
    private int[] counts;
    private int total;

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
        this.min = min;
        this.max = max;
        width = Math.max((max - min + 1) / buckets, 1);
        counts = new int[buckets];
        total = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int i = Math.min((v - min) / width, buckets - 1);
        counts[i]++;
        total++;
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
    	// some code goes here
        if (v > max) {
            if (op == Predicate.Op.GREATER_THAN || op == Predicate.Op.GREATER_THAN_OR_EQ || op == Predicate.Op.EQUALS) {
                return 0.0;
            }
            return 1.0;
        } else if (v < min) {
            if (op == Predicate.Op.LESS_THAN || op == Predicate.Op.LESS_THAN_OR_EQ || op == Predicate.Op.EQUALS) {
                return 0.0;
            }
            return 1.0;
        }
        int i = Math.min((v - min) / width, buckets - 1);
        int height = counts[i];

        double selectivity = 0;

        if (op == Predicate.Op.LESS_THAN_OR_EQ || op == Predicate.Op.GREATER_THAN_OR_EQ || op == Predicate.Op.EQUALS || op == Predicate.Op.NOT_EQUALS) {
            selectivity = ((double) height / width) / total;
        }

        if (op == Predicate.Op.NOT_EQUALS) {
            selectivity = 1 - selectivity;
        } else if (op == Predicate.Op.GREATER_THAN || op == Predicate.Op.GREATER_THAN_OR_EQ) {
            double bF = (double) height / total;
            int bRight = min + (i + 1) * width - 1;
            double bPart = (double) (bRight - v) / width;
            selectivity += bF * bPart;
            for (int j = i + 1; j < buckets; j++) {
                selectivity += ((double) counts[j] / total);
            }
        } else if (op == Predicate.Op.LESS_THAN || op == Predicate.Op.LESS_THAN_OR_EQ) {
            double bF = (double) height / total;
            int bLeft = min + i * width;
            double bPart = (double) (v - bLeft) / width;
            selectivity += bF * bPart;
            for (int j = 0; j < i; j++) {
                selectivity += ((double) counts[j] / total);
            }
        }

        return selectivity;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity() {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        String s = "";
        for (int i = 0; i < counts.length; i++) {
            s += String.format("bucket %d: ", i);
            s += counts[i];
            if (i < counts.length - 1) {
                s += " ";
            }
        }
        return s;
    }
}
