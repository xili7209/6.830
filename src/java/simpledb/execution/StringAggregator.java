package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

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
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        gbHandler =new CountHandler();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        String key = gbfield == NO_GROUPING ? NO_GROUPING_KEY : tup.getField(gbfield).toString();
        gbHandler.handle(key,tup.getField(afield));
    }



    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        HashMap<String, Integer> gbResults = gbHandler.getGbResult();
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
        HashMap<String,Integer> gbResult;
        abstract void handle(String key,Field field);
        private GBHandler(){
            gbResult = new HashMap<>();
        }
        public HashMap<String,Integer> getGbResult(){
            return gbResult;
        }
    }
    private class CountHandler extends GBHandler {
        @Override
        public void handle(String key, Field field) {
            if(gbResult.containsKey(key)){
                gbResult.put(key,gbResult.get(key)+1);
            }else{
                gbResult.put(key,1);
            }
        }
    }

}
