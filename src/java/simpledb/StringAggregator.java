package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * string只会求count
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

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

    private Map<Field, Integer> g2a;


    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what, TupleDesc td) {
        // some code goes here
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("String只支持COUNT类型");
        }
        this.gindex = gbfield;
        this.gbfieldType = gbfieldtype;
        this.aindex = afield;
        this.op = what;
        this.td = td;
        g2a = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gfield = null;
        Field aggreField = tup.getField(aindex);
        if (aggreField.getType() != Type.STRING_TYPE) {
            throw new IllegalArgumentException("该tuple指定的聚合列不是StringType");
        }
        if (gindex != Aggregator.NO_GROUPING) {
            gfield = tup.getField(gindex);
        }

        // 初始化 original tuple desc，确保每次都是相同的
        if (origTd == null) {
            origTd = tup.getTupleDesc();
        } else if (!origTd.equals(tup.getTupleDesc())) {
            throw new IllegalArgumentException("聚合的tuple desc不一致");
        }

        g2a.put(gfield, g2a.getOrDefault(gfield, 0) + 1);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
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
