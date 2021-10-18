package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static simpledb.execution.Aggregator.Op.MIN;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private static final String NO_GROUPING_KEY = "NO_GROUPING_KEY";
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private GBHandler gbHandler;
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
        this.what = what;
        switch (what){
            case MIN:
                gbHandler = new MinHandler();
                break;
            case SUM:
                gbHandler = new SumHandler();
                break;
            case COUNT:
                gbHandler = new CountHandler();
                break;
            case AVG:
                gbHandler = new AvgHandler();
                break;
            case MAX:
                gbHandler = new MaxHandler();
                break;

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
        String key = gbfield == NO_GROUPING ? NO_GROUPING_KEY : String.valueOf(tup.getField(gbfield));
        gbHandler.handle(key,tup.getField(afield));

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
        HashMap<String, Integer> gbResults = gbHandler.getGbResults();
        List<Tuple> tuples = new ArrayList<>();
        TupleDesc tupleDesc;
        Type[] types;
        String[] names;
        if(gbfield==NO_GROUPING){
            types = new Type[] {Type.INT_TYPE};
            names = new String[]{"aggregateVal"};
            tupleDesc = new TupleDesc(types,names);
            for(Integer value:gbResults.values()){
                Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0,new IntField(value));
                tuples.add(tuple);
            }

        }else{
            types = new Type[] {gbfieldtype,Type.INT_TYPE};
            names = new String[]{"groupVal","aggregateVal"};
            tupleDesc = new TupleDesc(types,names);
            for(Map.Entry<String,Integer> entry:gbResults.entrySet()){
                Tuple tuple = new Tuple(tupleDesc);
                if(gbfieldtype==Type.INT_TYPE){
                    tuple.setField(0,new IntField(Integer.parseInt(entry.getKey())));
                }else{
                    tuple.setField(0,new StringField(entry.getKey(),entry.getKey().length()));
                }
                tuple.setField(1,new IntField(entry.getValue()));
                tuples.add(tuple);
            }

        }
        return new TupleIterator(tupleDesc,tuples);
    }

    private abstract class GBHandler{
        public abstract void handle(String key, Field field);
        protected HashMap<String,Integer> gbResults;
        private GBHandler(){
            gbResults = new HashMap<>();
        }
        public HashMap<String,Integer> getGbResults(){
            return gbResults;
        }
    }

    private class CountHandler extends GBHandler{

        @Override
        public void handle(String key, Field field) {
            if(gbResults.containsKey(key)){
                gbResults.put(key,gbResults.get(key)+1);
            }else{
                gbResults.put(key,1);
            }
        }
    }

    private class SumHandler extends GBHandler{

        @Override
        public void handle(String key, Field field) {
            if(gbResults.containsKey(key)){
                gbResults.put(key,gbResults.get(key)+Integer.parseInt(field.toString()));
            }else{
                gbResults.put(key,Integer.parseInt(field.toString()));
            }
        }
    }
    private class MaxHandler extends GBHandler{

        @Override
        public void handle(String key, Field field) {
            int now = Integer.parseInt(field.toString());
            if(gbResults.containsKey(key)){
                gbResults.put(key,Math.max(now,gbResults.get(key)));
            }else{
                gbResults.put(key,now);
            }
        }
    }
    private class AvgHandler extends GBHandler{
        HashMap<String,Integer> sum;
        HashMap<String,Integer> count;
        private AvgHandler(){
            sum = new HashMap<>();
            count = new HashMap<>();
        }

        @Override
        public void handle(String key, Field field) {
            int now = Integer.parseInt(field.toString());
            if(gbResults.containsKey(key)){
                count.put(key,count.get(key)+1);
                sum.put(key,sum.get(key)+now);
            }else{
                count.put(key,1);
                sum.put(key,now);
            }
            gbResults.put(key,sum.get(key)/count.get(key));
        }
    }
    private class MinHandler extends GBHandler{

        @Override
        public void handle(String key, Field field) {
            int now = Integer.parseInt(field.toString());
            if(gbResults.containsKey(key)){
                gbResults.put(key,Math.min(now,gbResults.get(key)));
            }else{
                gbResults.put(key,now);
            }
        }
    }

}
