package simpledb;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int min;
    private int max;
    private int buckets;
    private int[] histogram;

    private int ntups;
    private int width;

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        width = (int) Math.ceil((double) (max - min + 1) / buckets);
        histogram = new int[buckets];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        int index = valueToIndex(v);
        if (index == -1) return;
        histogram[index]++;
        ntups++;
    }

    /**
     * @return -1表示在统计去见外
     */
    private int valueToIndex(int v) {
        if (v < min || max < v) return -1;
        if (v == max) {
            return buckets - 1;
        } else {
            return (v - min) / width;
        }
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        // some code goes here
        int index = valueToIndex(v);
        double left = width * index + min;
        double right = left + width - 1;
        double count;
        switch (op) {
            case EQUALS:
                if (index == -1) return 0;
                else return (double) histogram[index] / width / ntups;
            case GREATER_THAN:
                if (v < min) return 1;
                if (v > max) return 0;
                count = (double) histogram[index] * ((right - v) / width);
                for (int i = index+1; i < histogram.length; i++) {
                    count += histogram[i];
                }
                return count / ntups;
            case LESS_THAN:
                if (v < min) return 0;
                if (v > max) return 1;
                count = (double) histogram[index] * ((v - left) / width);
                for (int i = index-1; i >= 0; i--) {
                    count += histogram[i];
                }
                return count / ntups;
            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.LESS_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.GREATER_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
            case LIKE:
                // 应该在StringHistogram里实现的，因为不是IntHistogram的逻辑
                // 但是StringHistogram直接调用IntHistogram，所以需要在这里实现
                return avgSelectivity();
            case NOT_EQUALS:
                return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
            default:
                throw new IllegalArgumentException("Should not reach hear");
        }
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
