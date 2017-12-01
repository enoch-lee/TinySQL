import storageManager.*;
import java.util.*;

public class QueryHelper {
    //general sorting
    public static ArrayList<Tuple> onePassSort(Relation relation, MainMemory memory){
        int numOfBlocks = relation.getNumOfBlocks();
        relation.getBlocks(0, 0, numOfBlocks);
        ArrayList<Tuple> tuples = memory.getTuples(0, numOfBlocks);
        tuples.sort(new TupleComparator());
        clearMainMem(memory);
        return tuples;
    }

    //sorting relation by certain field
    public static ArrayList<Tuple> onePassSort(Relation relation, String fieldName, MainMemory memory){
        int numOfBlocks = relation.getNumOfBlocks();
        relation.getBlocks(0, 0, numOfBlocks);
        ArrayList<Tuple> tuples = memory.getTuples(0, numOfBlocks);
        tuples.sort(new TupleComparator(fieldName));
        clearMainMem(memory);
        return tuples;
    }

    //general main memory sorting
    public static ArrayList<Tuple> onePassSort(MainMemory memory, int numOfBlocks){
        ArrayList<Tuple> tuples = memory.getTuples(0, numOfBlocks);
        tuples.sort(new TupleComparator());
        clearMainMem(memory);
        return tuples;
    }

    //sorting main memory by certain field
    public static ArrayList<Tuple> onePassSort(MainMemory memory, String fieldName, int numOfBlocks){
        ArrayList<Tuple> tuples = memory.getTuples(0, numOfBlocks);
        tuples.sort(new TupleComparator(fieldName));
        clearMainMem(memory);
        return tuples;
    }

    public static void twoPassHelper(Relation relation, MainMemory memory, String fieldName){
        int numOfBlocks = relation.getNumOfBlocks(),  sortedBlocks = 0;
        ArrayList<Tuple> tuples;
        while(sortedBlocks < numOfBlocks){
            int t = Math.min(memory.getMemorySize(), numOfBlocks - sortedBlocks);
            relation.getBlocks(sortedBlocks, 0, t);
            //sort main memory
            tuples = onePassSort(memory, fieldName, t);
            memory.setTuples(0, tuples);
            relation.setBlocks(sortedBlocks, 0, t);
            //t <= memory.getMemorySize() ---> error!!!!(When numOfBlocks > 10)
            if(t < memory.getMemorySize()) {
                break;
            }else{
                sortedBlocks += memory.getMemorySize();
            }
            clearMainMem(memory);
        }
    }

    public static ArrayList<Tuple> twoPassSort(Relation relation, MainMemory memory, String fieldName){
        //phase 1: making sorted sublists
        twoPassHelper(relation, memory, fieldName);

        //phase 2: merging
        int numOfBlocks = relation.getNumOfBlocks();
        ArrayList<Tuple> res = new ArrayList<>();
        ArrayList<ArrayList<Tuple>> tuples = new ArrayList<>();
        ArrayList<Pair<Integer, Integer>> blockIndexOfSublists = new ArrayList<>();

        //bring in a block from each of the sorted sublists
        for(int i = 0, j = 0; i < numOfBlocks; i += memory.getMemorySize(), j++){
            //initial index must be i + 1
            blockIndexOfSublists.add(new Pair<>(i + 1, Math.min(i + memory.getMemorySize(), numOfBlocks)));
            relation.getBlock(i, j);
            tuples.add(memory.getTuples(j, 1));
        }

        for(int k = 0; k < relation.getNumOfTuples(); ++k){
            for(int i = 0; i < blockIndexOfSublists.size(); ++i){
                //read in the next block from a sublist if its block is exhausted
                if(tuples.get(i).isEmpty() && (blockIndexOfSublists.get(i).first < blockIndexOfSublists.get(i).second)){
                    relation.getBlock(blockIndexOfSublists.get(i).first, i);
                    tuples.set(i, memory.getTuples(i, 1));
                    blockIndexOfSublists.get(i).first++;
                }
            }

            //find the smallest key among the first remaining elements of all the sublists
            ArrayList<Tuple> minTuples = new ArrayList<>();
            for(int j = 0; j < tuples.size(); ++j){
                if(!tuples.isEmpty() && !tuples.get(j).isEmpty()) minTuples.add(tuples.get(j).get(0));
            }
            Tuple minTuple = Collections.min(minTuples, new TupleComparator(fieldName));
            res.add(minTuple);

            //remove the minimum element
            for(int j = 0; j < tuples.size(); ++j){
                if(!tuples.get(j).isEmpty() && tuples.get(j).get(0).equals(minTuple)) tuples.get(j).remove(0);
            }
        }

        for(Tuple tuple : res) System.out.println(tuple);
        clearMainMem(memory);
        return res;
    }

    public static Relation distinct(SchemaManager schemaManager, Relation relation, MainMemory memory, String fieldName){
        Schema schema = relation.getSchema();
        String name = relation.getRelationName() + "_distinct_" + fieldName;
        if(schemaManager.relationExists(name)) schemaManager.deleteRelation(name);
        Relation tempRelation = schemaManager.createRelation(name, schema);
        ArrayList<Tuple> tuples;
        if(relation.getNumOfBlocks() <= memory.getMemorySize()){
            tuples = onePassRemoveDuplicate(relation, memory, fieldName);
        }else{
            tuples = twoPassRemoveDuplicate(relation, memory, fieldName);
        }

        int tupleNumber = tuples.size();
        int tuplesPerBlock = schema.getTuplesPerBlock();
        int tupleBlocks = 0;
        if(tupleNumber < tuplesPerBlock){
            tupleBlocks = 1;
        }else if(tupleNumber > tuplesPerBlock && tupleNumber % tuplesPerBlock == 0){
            tupleBlocks = tupleNumber / tuplesPerBlock;
        }else{
            tupleBlocks = tupleNumber / tuplesPerBlock + 1;
        }

        int index = 0;
        while(index < tupleBlocks){
            int t = Math.min(memory.getMemorySize(), tupleBlocks - index);
            for(int i = 0; i < t; i++){
                Block block = memory.getBlock(i);
                block.clear();
                for(int j = 0; j< tuplesPerBlock; j++){
                    if(!tuples.isEmpty()){
                        Tuple temp = tuples.get(0);
                        block.setTuple(j, temp);
                        tuples.remove(temp);
                    }else{
                        break;
                    }
                }
            }
            tempRelation.setBlocks(index,0, t);
            if(t < memory.getMemorySize()){
                break;
            }else{
                index += memory.getMemorySize();
            }
        }

        return tempRelation;
    }

    public static ArrayList<Tuple> onePassRemoveDuplicate(Relation relation, MainMemory memory, String fieldName){
        ArrayList<Tuple> res = new ArrayList<>();
        HashSet<String> hashSet = new HashSet<>();
        int numOfBlocks = relation.getNumOfBlocks();
        relation.setBlocks(0, 0, numOfBlocks);
        ArrayList<Tuple> tuples = memory.getTuples(0, numOfBlocks);
        for(Tuple tuple : tuples){
            if(tuple.getField(fieldName).type.equals(FieldType.STR20)){
                if(hashSet.add(tuple.getField(fieldName).str)) res.add(tuple);
            }else{
                if(hashSet.add(Integer.toString(tuple.getField(fieldName).integer))) res.add(tuple);
            }
        }
        clearMainMem(memory);
        return res;
    }

    public static ArrayList<Tuple> twoPassRemoveDuplicate(Relation relation, MainMemory memory, String fieldName){
        //phase 1: making sorted sublists
        twoPassHelper(relation, memory, fieldName);

        //phase 2
        int numOfBlocks = relation.getNumOfBlocks();
        HashSet<String> hashSet = new HashSet<>();
        ArrayList<Tuple> res = new ArrayList<>();
        ArrayList<ArrayList<Tuple>> tuples = new ArrayList<>();
        ArrayList<Pair<Integer, Integer>> blockIndexOfSublists = new ArrayList<>();

        //bring in a block from each of the sorted sublists
        for(int i = 0, j = 0; i < numOfBlocks; i += memory.getMemorySize(), j++){
            //initial index must be i + 1
            blockIndexOfSublists.add(new Pair<>(i + 1, Math.min(i + memory.getMemorySize(), numOfBlocks)));
            relation.getBlock(i, j);
            tuples.add(memory.getTuples(j, 1));
        }

        for(int k = 0; k < relation.getNumOfTuples(); ++k){
            for(int i = 0; i < blockIndexOfSublists.size(); ++i){
                //read in the next block form a sublist if its block is exhausted
                if(tuples.get(i).isEmpty() && (blockIndexOfSublists.get(i).first < blockIndexOfSublists.get(i).second)){
                    relation.getBlock(blockIndexOfSublists.get(i).first, i);
                    tuples.set(i, memory.getTuples(i, 1));
                    blockIndexOfSublists.get(i).first++;
                }
            }

            //find the smallest key among the first remaining elements of all the sublists
            ArrayList<Tuple> minTuples = new ArrayList<>();
            for(int j = 0; j < tuples.size(); ++j){
                if(!tuples.isEmpty() && !tuples.get(j).isEmpty()) minTuples.add(tuples.get(j).get(0));
            }

            //the first difference to twoPassSort -
            //multiple elements could be removed in one loop, the number of loops could be less than numOfBlocks
            //so the loop could break earlier
            if(minTuples.isEmpty()) break;

            Tuple minTuple = Collections.min(minTuples, new TupleComparator(fieldName));
            //the 2nd difference - use Hashset
            if(minTuple.getField(fieldName).type.equals(FieldType.STR20)){
                if(hashSet.add(minTuple.getField(fieldName).str)) res.add(minTuple);
            }else{
                if(hashSet.add(Integer.toString(minTuple.getField(fieldName).integer))) res.add(minTuple);
            }

            //the 3rd difference - remove all minimum elements
            for(int j = 0; j < tuples.size(); ++j){
                if(!tuples.get(j).isEmpty()) {
                    if(tuples.get(j).get(0).getField(fieldName).type.equals(minTuple.getField(fieldName).type)){
                        if(tuples.get(j).get(0).getField(fieldName).type.equals(FieldType.STR20)){
                            if(tuples.get(j).get(0).getField(fieldName).str.equals(minTuple.getField(fieldName).str))
                                tuples.get(j).remove(0);
                        }else{
                            if(tuples.get(j).get(0).getField(fieldName).integer == minTuple.getField(fieldName).integer)
                                tuples.get(j).remove(0);
                        }
                    }
                }
            }
        }

        //for(Tuple tuple : res) System.out.println(tuple);
        clearMainMem(memory);
        return res;
    }

    //sort to fill holes
    public static void fillHoles(Relation relation, MainMemory memory){
        int numOfBlocks = relation.getNumOfBlocks(),  sortedBlocks = 0;
        ArrayList<Tuple> tuples;
        while(sortedBlocks < numOfBlocks){
            int t = Math.min(memory.getMemorySize(), numOfBlocks - sortedBlocks);
            relation.getBlocks(sortedBlocks, 0, t);
            tuples = onePassSort(memory, t);
            memory.setTuples(0, tuples);
            relation.setBlocks(sortedBlocks, 0, t);
            //t <= memory.getMemorySize() ---> error!!!!(When numOfBlocks > 10)
            if(t < memory.getMemorySize()) {
                break;
            }else{
                sortedBlocks += memory.getMemorySize();
            }
            clearMainMem(memory);
        }
    }

    public static void clearMainMem(MainMemory memory){
        for(int i = 0; i < memory.getMemorySize(); ++i) memory.getBlock(i).clear();
    }
}

class Pair<A, B> {
    public A first;
    public B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }
}






