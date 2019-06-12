package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gindex;
    private Type gbfieldType;
    private int aindex;
    private Op op;

    // 聚合结果的tuple desc
    // [group_by_filed] aggregate_field
    private TupleDesc td;

    // 保存原始的tuple desc
    private TupleDesc origTd;

    // group的filed和结果，其实field的hashcode就是value
    // 也可以直接写成 map<int, int>，group by key的值
    // 例如：
    // 1,1 1,2 1,3 2,1 2,2
    // min group之后 -> 1,1 2,1
    private Map<Field, Integer> g2a;

    // 相当于保存了所有的结果，用来求平均值
    private Map<Field, List<Integer>> g2as;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what, TupleDesc td) {
        // some code goes here
        this.gindex = gbfield;
        this.gbfieldType = gbfieldtype;
        this.aindex = afield;
        this.op = what;
        this.td = td;
        g2a = new HashMap<>();
        g2as = new HashMap<>();
    }

    /**
     * NOTE(hgao): 就是对tuple进行group操作
     * todo ? 如果是NO_GROUPING类型，那么不应该平铺吗，这里都将key设置为null，也就是最后只剩下一个key=null的值
     * <p>
     * 1. 构造group field
     * <p>
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gfield = null;
        int aggreValue = ((IntField) tup.getField(aindex)).getValue();
        int newVal = aggreValue;
        if (gindex != Aggregator.NO_GROUPING) {
            gfield = tup.getField(gindex);
        }

        // 初始化 original tuple desc，确保每次都是相同的
        if (origTd == null) {
            origTd = tup.getTupleDesc();
        } else if (!origTd.equals(tup.getTupleDesc())) {
            throw new IllegalArgumentException("聚合的tuple desc不一致");
        }

        if (!g2as.containsKey(gfield)) {
            g2as.put(gfield, new ArrayList<>());
        }
        g2as.get(gfield).add(aggreValue);

        if (op == Op.AVG) {
            // 计算avg
            int sum = g2as.get(gfield).stream().reduce(0, (a, b) -> a + b);
            newVal = sum / g2as.get(gfield).size();
        } else if (op == Op.COUNT) {
            newVal = g2a.getOrDefault(gfield, 0) + 1;
        } else {
            // 其他操作
            if (g2a.containsKey(gfield)) {
                newVal = calcNewValue(g2a.get(gfield), newVal, op);
            }
        }

        g2a.put(gfield, newVal);
    }

    private int calcNewValue(int oldVal, int newVal, Op op) {
        switch (op) {
            case MIN:
                return Math.min(oldVal, newVal);
            case SUM:
                return oldVal + newVal;
            case MAX:
                return Math.max(oldVal, newVal);
            default:
                throw new IllegalArgumentException("operator not supported.");
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Field, Integer> kv : g2a.entrySet()) {
            Tuple t = new Tuple(td);
            if (gindex == Aggregator.NO_GROUPING) {
                t.setField(0, new IntField(kv.getValue()));
            } else {
                t.setField(0, kv.getKey());
                t.setField(1, new IntField(kv.getValue()));
            }
            tuples.add(t);
        }
        return new TupleIterator(td, tuples);
    }

}
