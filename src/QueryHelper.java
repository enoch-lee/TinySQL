import storageManager.*;
import java.util.*;

public class QueryHelper {
    private static String fileName;

    //sort relations
    public static Relation sort(SchemaManager schemaManager, Relation relation, MainMemory memory, String fieldName){
        String name = relation.getRelationName() + "_sortBy_" + fieldName;

        if(schemaManager.relationExists(name)) schemaManager.deleteRelation(name);
        ArrayList<Tuple> tuples;
        if(relation.getNumOfBlocks() <= memory.getMemorySize()){
            tuples = onePassSort(relation, memory, fieldName);
        }else{
            tuples = twoPassSort(relation, memory, fieldName);
        }
        clearMainMem(memory);
        return createRelationFromTuples(tuples, name, schemaManager, relation, memory);
    }

    //eliminate duplicate tuples
    public static Relation distinct(SchemaManager schemaManager, Relation relation, MainMemory memory, String fieldName){
        String name = relation.getRelationName() + "_distinct_" + fieldName;
        if(schemaManager.relationExists(name)) schemaManager.deleteRelation(name);
        ArrayList<Tuple> tuples;
        if(relation.getNumOfBlocks() <= memory.getMemorySize()){
            tuples = onePassRemoveDuplicate(relation, memory, fieldName);
        }else{
            tuples = twoPassRemoveDuplicate(relation, memory, fieldName);
        }
        clearMainMem(memory);
        return createRelationFromTuples(tuples, name, schemaManager, relation, memory);
    }

    //selection
    public static Relation select(SchemaManager schemaMG, Relation relation, MainMemory memory, ParseTree parseTree){
        ArrayList<Tuple> tuples = new ArrayList<>();
        int numOfBlocks = relation.getNumOfBlocks(), memoryBlocks = memory.getMemorySize();

        if(numOfBlocks <= memoryBlocks){
            tuples = selectQueryHelper(parseTree, relation, memory,0, numOfBlocks);
        }else{
            int remainNumber = numOfBlocks;
            int relationindex = 0;
            ArrayList<Tuple> tmp;
            while(remainNumber > memoryBlocks){
                tmp = selectQueryHelper(parseTree, relation, memory, relationindex, memoryBlocks);
                tuples.addAll(tmp);
                remainNumber = remainNumber - memoryBlocks;
                relationindex = relationindex + memoryBlocks;
            }
            tmp = selectQueryHelper(parseTree, relation, memory, relationindex, remainNumber);
            tuples.addAll(tmp);
        }
        String name = relation.getRelationName() + "_select_";
        if(schemaMG.relationExists(name)) schemaMG.deleteRelation(name);
        clearMainMem(memory);
        return createRelationFromTuples(tuples, name, schemaMG, relation, memory);
    }

    //get blocks once to reduce disk timer
    public static ArrayList<Tuple> selectQueryHelper(ParseTree parseTree, Relation relation, MainMemory memory, int relationIndex, int loop ){
        Block block;
        ArrayList<Tuple> res = new ArrayList<>();
        relation.getBlocks(relationIndex, 0, loop);
        for(int i=0; i<loop; i++){
            block = memory.getBlock(i);
            ArrayList<Tuple> tuples = block.getTuples();
            for(Tuple tuple : tuples){
                if(parseTree.where){
                    if(parseTree.expressionTree.checkTuple(tuple)) res.add(tuple);
                }else{
                    //System.out.println(tuple);
                    res.add(tuple);
                }
            }
        }
        clearMainMem(memory);
        return res;
    }

    //projection
    public static void project(Relation relation, MainMemory memory, ParseTree parseTree){

        int numOfBlocks = relation.getNumOfBlocks();
        int i = 0;
        if(parseTree.attributes.get(0).equals("*")){
            Query.writeFile(relation.getSchema().getFieldNames().toString(), true);
            //System.out.print(relation.getSchema().getFieldNames());
        }else{
            for(String attr : parseTree.attributes){
                Query.writeFile(attr + " ", false);
                //System.out.print(attr + " ");
            }
        }
        //System.out.println();
        while(i < numOfBlocks){
            int t = Math.min(memory.getMemorySize(), numOfBlocks - i);
            relation.getBlocks(i, 0, t);
            if(memory.getBlock(0).isEmpty()){
                System.out.println("No Selected Tuples");
                return;
            }
            projectHelper(memory, parseTree, t);
            if(t < memory.getMemorySize()) break;
            else i += memory.getMemorySize();
        }
        clearMainMem(memory);
    }

    private static void projectHelper(MainMemory memory, ParseTree parseTree, int memBlocks){
        ArrayList<Tuple> tuples = memory.getTuples(0, memBlocks);
        for(Tuple tuple : tuples){
            if(tuple.isNull()) return;
            if(parseTree.attributes.get(0).equals("*")){
                for(int i = 0; i < tuple.getNumOfFields(); ++i){
                    if(tuple.getField(i).type.equals(FieldType.INT) && tuple.getField(i).integer == Integer.MIN_VALUE){
                        Query.writeFile("NULL ", false);
                        //System.out.print("NULL ");
                    }else{
                        Query.writeFile(tuple.getField(i)+ " ", false);
                        //System.out.print(tuple.getField(i)+ " ");
                    }
                }
                //System.out.println();
                Query.writeFile("\r\n", false);
            }else{
                for(String attr : parseTree.attributes){
                    //handle course.grade
                    if(attr.contains(".") && parseTree.tables.size() == 1) attr = attr.split("\\.")[1];
                    //handle NULL case
                    if(tuple.getField(attr).type.equals(FieldType.INT) && tuple.getField(attr).integer == Integer.MIN_VALUE){
                        Query.writeFile("NULL ", false);
                        //System.out.print("NULL ");
                    }else{
                        Query.writeFile(tuple.getField(attr) + " ", false);
                        //System.out.print(tuple.getField(attr) + " ");
                    }
                }
                //System.out.println();
                Query.writeFile("\r\n", false);
            }
        }
        clearMainMem(memory);
    }

    //create temporary relation from tuples
    public static Relation createRelationFromTuples(ArrayList<Tuple> tuples, String name, SchemaManager schemaManager, Relation relation, MainMemory memory){
        Schema schema = relation.getSchema();
        if(schemaManager.relationExists(name)) schemaManager.deleteRelation(name);
        Relation tempRelation = schemaManager.createRelation(name, schema);
        int tupleNumber = tuples.size(), tuplesPerBlock = schema.getTuplesPerBlock();
        int tupleBlocks;
        if(tupleNumber < tuplesPerBlock){
            tupleBlocks = 1;
        }else if(tupleNumber >= tuplesPerBlock && tupleNumber % tuplesPerBlock == 0){
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
        clearMainMem(memory);
        return tempRelation;
    }

    //general sorting
    private static ArrayList<Tuple> onePassSort(Relation relation, MainMemory memory){
        int numOfBlocks = relation.getNumOfBlocks();
        relation.getBlocks(0, 0, numOfBlocks);
        ArrayList<Tuple> tuples = memory.getTuples(0, numOfBlocks);
        tuples.sort(new TupleComparator());
        clearMainMem(memory);
        return tuples;
    }

    //sorting relation by certain field
    private static ArrayList<Tuple> onePassSort(Relation relation, MainMemory memory, String fieldName){
        int numOfBlocks = relation.getNumOfBlocks();
        relation.getBlocks(0, 0, numOfBlocks);
        ArrayList<Tuple> tuples = memory.getTuples(0, numOfBlocks);
        tuples.sort(new TupleComparator(fieldName));
        clearMainMem(memory);
        return tuples;
    }

    //general main memory sorting
    private static ArrayList<Tuple> onePassSort(MainMemory memory, int numOfBlocks){
        ArrayList<Tuple> tuples = memory.getTuples(0, numOfBlocks);
        tuples.sort(new TupleComparator());
        clearMainMem(memory);
        return tuples;
    }

    //sorting main memory by certain field
    private static ArrayList<Tuple> onePassSort(MainMemory memory, String fieldName, int numOfBlocks){
        ArrayList<Tuple> tuples = memory.getTuples(0, numOfBlocks);
        tuples.sort(new TupleComparator(fieldName));
        clearMainMem(memory);
        return tuples;
    }

    //first phase of two pass algorithm, sorted by fieldName
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
            //t <= memory.getMemorySize() ---> error!!!!(when t == 10)
            if(t < memory.getMemorySize()) {
                break;
            }else{
                sortedBlocks += memory.getMemorySize();
            }
            clearMainMem(memory);
        }
    }

    //first phase of two pass algorithm, general sort
    public static void twoPassHelper(Relation relation, MainMemory memory){
        int numOfBlocks = relation.getNumOfBlocks(),  sortedBlocks = 0;
        ArrayList<Tuple> tuples;
        while(sortedBlocks < numOfBlocks){
            int t = Math.min(memory.getMemorySize(), numOfBlocks - sortedBlocks);
            relation.getBlocks(sortedBlocks, 0, t);
            //sort main memory
            tuples = onePassSort(memory, t);
            memory.setTuples(0, tuples);
            relation.setBlocks(sortedBlocks, 0, t);
            //t <= memory.getMemorySize() ---> error!!!!(when t == 10)
            if(t < memory.getMemorySize()) {
                break;
            }else{
                sortedBlocks += memory.getMemorySize();
            }
            clearMainMem(memory);
        }
    }

    private static ArrayList<Tuple> twoPassSort(Relation relation, MainMemory memory, String fieldName){
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
        //for(Tuple tuple : res) System.out.println(tuple);
        clearMainMem(memory);
        return res;
    }


    private static ArrayList<Tuple> onePassRemoveDuplicate(Relation relation, MainMemory memory, String fieldName){
        ArrayList<Tuple> res = new ArrayList<>();
        int numOfBlocks = relation.getNumOfBlocks();
        relation.getBlocks(0, 0, numOfBlocks); //relation.setBlocks() --> error!!!!!!
        ArrayList<Tuple> tuples = memory.getTuples(0, numOfBlocks);
        //handle case SELECT DISTINCT * FROM course
        if(fieldName.equals("*")){
            HashSet<MyTuple> hashSet = new HashSet<>();
            for(Tuple tuple : tuples){
                if(hashSet.add(new MyTuple(tuple))) res.add(tuple);
            }
        }else{
            HashSet<String> hashSet = new HashSet<>();
            for(Tuple tuple : tuples){
                if(tuple.getField(fieldName).type.equals(FieldType.STR20)){
                    if(hashSet.add(tuple.getField(fieldName).str)) res.add(tuple);
                }else{
                    if(hashSet.add(Integer.toString(tuple.getField(fieldName).integer))) res.add(tuple);
                }
            }
        }
        clearMainMem(memory);
        return res;
    }

    private static ArrayList<Tuple> twoPassRemoveDuplicate(Relation relation, MainMemory memory, String fieldName){
        //phase 1: making sorted sublists
        HashSet<MyTuple> hashSetTuple = new HashSet<>();
        HashSet<String> hashSet = new HashSet<>();
        if(fieldName.equals("*")){
            twoPassHelper(relation, memory);
            hashSetTuple = new HashSet<>();
        }else{
            twoPassHelper(relation, memory, fieldName);
            hashSet = new HashSet<>();
        }

        //phase 2
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

            if(fieldName.equals("*")){
                Tuple minTuple = Collections.min(minTuples, new TupleComparator());
                if(hashSetTuple.add(new MyTuple(minTuple))) res.add(minTuple);
                for(ArrayList<Tuple> tuple : tuples){
                    if(!tuple.isEmpty()) {
                        if(new MyTuple(tuple.get(0)).equals(new MyTuple(minTuple))) tuple.remove(0);
                    }
                }
            }else{
                Tuple minTuple = Collections.min(minTuples, new TupleComparator(fieldName));
                //the 2nd difference - use Hashset
                if(hashSet.add(minTuple.getField(fieldName).toString())) res.add(minTuple);
                //the 3rd difference - remove all minimum elements
                for(ArrayList<Tuple> tuple : tuples){
                    if(!tuple.isEmpty()) {
                        if(tuple.get(0).getField(fieldName).type.equals(FieldType.STR20)){
                            if(tuple.get(0).getField(fieldName).str.equals(minTuple.getField(fieldName).str))
                                tuple.remove(0);
                        }else{
                            if(tuple.get(0).getField(fieldName).integer == minTuple.getField(fieldName).integer)
                                tuple.remove(0);
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
            if(tuples.isEmpty()) break;  //handle case that the relation is empty but it still have empty blocks
            memory.setTuples(0, tuples);
            relation.setBlocks(sortedBlocks, 0, t);
            //t <= memory.getMemorySize() ---> error!!!!(When numOfBlocks > 10)
            if(t < memory.getMemorySize()) {
                break;
            }else{
                sortedBlocks += memory.getMemorySize();
            }
        }
        //remove empty blocks of the relation after sorting which could increase diskIO and diskTimer
        for(int i = 0; i < relation.getNumOfBlocks(); ++i){
            relation.getBlock(i, 0);
            if(memory.getBlock(0).isEmpty()) relation.deleteBlocks(i);
            memory.getBlock(0).clear();
        }
        clearMainMem(memory);
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

class MyTuple{

    public Tuple tuple;
    public String fields;
    MyTuple(Tuple tuple){
        this.tuple = tuple;
        this.fields = "";
        for(int i = 0; i < tuple.getNumOfFields(); ++i){
            fields += tuple.getField(i).toString();
        }
    }
    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof MyTuple)) return false;
        return this.hashCode() == obj.hashCode();
    }

    public int hashCode() {
        return this.fields.hashCode();
    }

}




