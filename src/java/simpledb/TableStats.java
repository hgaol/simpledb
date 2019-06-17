package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p>
 * This class is not needed in implementing proj1 and proj2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;
    private int ioCostPerPage;
    // 其实应该改为DbFile，不一定都是heap file，之后如果实现b+tree呢
    private HeapFile dbFile;
    private TupleDesc td;
    private int ntups;
    // Key是该表的每一列的FieldName，Value是最小值和最大值的数组
    private HashMap<String, Integer[]> attrs;
    // Key是该表的每一列的FieldName，保存int hist或者string hist
    private HashMap<String, Object> name2hist;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(HashMap<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.ioCostPerPage = ioCostPerPage;
        dbFile = (HeapFile) Database.getCatalog().getDbFile(tableid);
        td = dbFile.getTupleDesc();
        attrs = new HashMap<>();
        name2hist = new HashMap<>();
        Transaction t = new Transaction();//查询计划的Transaction是在这里新建的
        DbFileIterator iter = dbFile.iterator(t.getId());
        process(iter);
    }

    /**
     * 计算table的tuple数量，计算每一个int类型的列的最大最小值，计算每一列的histogram
     * 需要两次scan是因为第一次要遍历出来int field列的最大最小值
     *
     * @param iter
     */
    private void process(DbFileIterator iter) {
        try {
            iter.open();
            // 这一轮主要计算出int field的最小最大值
            while (iter.hasNext()) {
                //计算tuple数量
                ntups++;
                Tuple tup = iter.next();
                for (int i = 0; i < td.numFields(); i++) {
                    Type type = td.getFieldType(i);
                    String fieldName = td.getFieldName(i);
                    // 只需要统计int field类型的min和max
                    if (type == Type.INT_TYPE) {
                        Integer[] minMax = attrs.getOrDefault(fieldName,
                                new Integer[]{Integer.MAX_VALUE, Integer.MIN_VALUE});
                        Integer val = ((IntField) tup.getField(i)).getValue();
                        minMax[0] = Math.min(minMax[0], val);
                        minMax[1] = Math.max(minMax[1], val);
                        attrs.put(fieldName, minMax);
                    }
                }
            }

            // 计算出min和max，可以初始化name2hist
            for (int i = 0; i < td.numFields(); i++) {
                Type type = td.getFieldType(i);
                String fieldName = td.getFieldName(i);
                if (type == Type.INT_TYPE) {
                    Integer[] minMax = attrs.get(fieldName);
                    IntHistogram hist = new IntHistogram(NUM_HIST_BINS, minMax[0], minMax[1]);
                    name2hist.put(fieldName, hist);
                } else if (type == Type.STRING_TYPE) {
                    StringHistogram hist = new StringHistogram(NUM_HIST_BINS);
                    name2hist.put(fieldName, hist);
                }
            }

            // 计算各个field的histgram
            iter.rewind();
            while (iter.hasNext()) {
                Tuple tup = iter.next();
                for (int i = 0; i < td.numFields(); i++) {
                    Type type = td.getFieldType(i);
                    String fieldName = td.getFieldName(i);
                    if (type == Type.INT_TYPE) {
                        Integer val = ((IntField) tup.getField(i)).getValue();
                        ((IntHistogram) name2hist.get(fieldName)).addValue(val);
                    } else if (type == Type.STRING_TYPE) {
                        String val = ((StringField) tup.getField(i)).getValue();
                        ((StringHistogram) name2hist.get(fieldName)).addValue(val);
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return dbFile.numPages() * ioCostPerPage;
    }

    /**
     * selectivity相当于经过各种filter之后，剩下的tuple占总tuple的百分比
     * 也就是要计算还剩多少tuples
     *
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     * selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) Math.ceil(totalTuples() * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     *
     * @param field the index of the field
     * @param op    the operator in the predicate
     *              The semantic of the method is that, given the table, and then given a
     *              tuple, of which we do not know the value of the field, return the
     *              expected selectivity. You may estimate this value from the histograms.
     */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     * predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        String fieldName = td.getFieldName(field);
        if (constant.getType() == Type.INT_TYPE) {
            int value = ((IntField) constant).getValue();
            IntHistogram histogram = (IntHistogram) name2hist.get(fieldName);
            return histogram.estimateSelectivity(op, value);
        } else {
            String value = ((StringField) constant).getValue();
            StringHistogram histogram = (StringHistogram) name2hist.get(fieldName);
            return histogram.estimateSelectivity(op, value);
        }
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        // some code goes here
        return ntups;
    }

}
