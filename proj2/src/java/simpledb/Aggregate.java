package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator child;
    private int afield;
    private int gfield;
    Aggregator.Op aop;

    // child tuple desc
    private TupleDesc childTd;

    // 真正执行聚合的类
    private Aggregator aggregator;

    // 聚合的结果通过这个iterator访问
    private DbIterator aggregateIter;

    // 聚合那一列的分组类型
    private Type groupType;

    // 聚合结果的tuple desc
    // [group_by_filed] aggregate_field
    private TupleDesc td;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The DbIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        this.childTd = child.getTupleDesc();
        Type aggreType = childTd.getFieldType(afield);
        groupType = gfield == Aggregator.NO_GROUPING ? null : childTd.getFieldType(gfield);
        if (aggreType == Type.INT_TYPE) {
            aggregator = new IntegerAggregator(gfield, groupType, afield, aop, getTupleDesc());
        } else if (aggreType == Type.STRING_TYPE) {
            aggregator = new StringAggregator(gfield, groupType, afield, aop, getTupleDesc());
        } else {
            throw new IllegalArgumentException("不支持的group type类型" + groupType);
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link simpledb.Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        if (gfield == Aggregator.NO_GROUPING) {
            return null;
        } else {
            return child.getTupleDesc().getFieldName(gfield);
        }
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();
        while (child.hasNext()) {
            aggregator.mergeTupleIntoGroup(child.next());
        }
        aggregateIter = aggregator.iterator();
        aggregateIter.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (aggregateIter.hasNext()) {
            return aggregateIter.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        aggregateIter.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        if (td == null) {
            td = getTupleDesc(child.getTupleDesc(), gfield, afield);
        }
        return td;
    }

    private TupleDesc getTupleDesc(TupleDesc tupleDesc, int gindex, int aindex) {
        Type[] types;
        String[] names;
        String afieldName = tupleDesc.getFieldName(aindex);;

        if (gindex == Aggregator.NO_GROUPING) {
            types = new Type[]{Type.INT_TYPE};
            names = new String[]{afieldName};
        } else {
            String gfieldName = tupleDesc.getFieldName(gindex);
            types = new Type[]{tupleDesc.getFieldType(gindex), Type.INT_TYPE};
            names = new String[]{gfieldName, afieldName};
        }

        return new TupleDesc(types, names);
    }

    public void close() {
        // some code goes here
        super.close();
        aggregateIter.close();
        child.close();
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        child = children[0];
    }

}
