import storageManager.*;
import java.util.ArrayList;
import java.util.Collections;

public class Join {
    @SuppressWarnings("Duplicates")
    public static Relation crossProduct(SchemaManager schema_manager, MainMemory memory, String tableOne, String tableTwo) {
        Relation new_relation;
        ArrayList<Tuple> tuples;

        //System.out.println("CrossJoin...\n");
        Relation TableOne = schema_manager.getRelation(tableOne);
        Relation TableTwo = schema_manager.getRelation(tableTwo);
        int sizeOne = TableOne.getNumOfBlocks();
        int sizeTwo = TableTwo.getNumOfBlocks();


        Relation smallRelation;
        if (sizeOne < sizeTwo) smallRelation = TableOne;
        else smallRelation = TableTwo;

        //One pass...
        if (smallRelation.getNumOfBlocks() < memory.getMemorySize()-1) {
            tuples = onePassJoin(schema_manager, memory, tableOne, tableTwo);
        }else{
            tuples = nestedLoopJoin(schema_manager, memory, tableOne, tableTwo);
        }

        Schema schema = combineSchema(schema_manager, tableOne, tableTwo);
        String name = tableOne+"_cross_"+tableTwo;

        if(schema_manager.relationExists(name)){
            schema_manager.deleteRelation(name);
        }
        new_relation = schema_manager.createRelation(name, schema);

        int tupleNumber = tuples.size();
        int tuplesPerBlock = schema.getTuplesPerBlock();
        int tupleBlocks;
        if(tupleNumber<tuplesPerBlock){
            tupleBlocks = 1;
        }else if(tupleNumber>tuplesPerBlock && tupleNumber%tuplesPerBlock==0){
            tupleBlocks = tupleNumber/tuplesPerBlock;
        }else{
            tupleBlocks = tupleNumber/tuplesPerBlock +1;
        }

        int sortedBlocks = 0;
        //sortedBlocks < memory.getMemorySize() --> error!!!!
        while(sortedBlocks < tupleBlocks){
            int t = Math.min(memory.getMemorySize(), tupleBlocks - sortedBlocks);
            for(int i=0; i<t; i++){
                Block block = memory.getBlock(i);
                block.clear();
                for(int j=0; j<tuplesPerBlock; j++){
                    if(!tuples.isEmpty()){
                        Tuple temp = tuples.get(0);
                        block.setTuple(j, temp);
                        tuples.remove(temp);
                    }else{
                        break;
                    }
                }
            }
            new_relation.setBlocks(sortedBlocks,0, t);
            if(t < memory.getMemorySize()){
                break;
            }else{
                sortedBlocks += memory.getMemorySize();
            }

        }
        return new_relation;
    }

    private static ArrayList<Tuple> onePassJoin(SchemaManager schema_manager, MainMemory memory, String tableOne, String tableTwo){
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        Relation TableOne = schema_manager.getRelation(tableOne);
        Relation TableTwo = schema_manager.getRelation(tableTwo);
        int sizeOne = TableOne.getNumOfBlocks();
        int sizeTwo = TableTwo.getNumOfBlocks();

        Relation smallRelation, largeRelation;
        if (sizeOne < sizeTwo) {
            smallRelation = TableOne;
            largeRelation = TableTwo;
        }
        else {
            smallRelation = TableTwo;
            largeRelation = TableOne;
        }

        Schema newSchema = combineSchema(schema_manager, tableOne, tableTwo);
        String newName="TempTable_"+tableOne+"_crossJoin_"+tableTwo;
        if(schema_manager.relationExists(newName)){
            schema_manager.deleteRelation(newName);
        }
        Relation newRelation = schema_manager.createRelation(newName, newSchema);

        //put small relation into the memory first
        smallRelation.getBlocks(0,0,smallRelation.getNumOfBlocks());

        for(int i=0; i<largeRelation.getNumOfBlocks(); i++){
            largeRelation.getBlock(i, memory.getMemorySize()-1);
            Block block = memory.getBlock(memory.getMemorySize()-1);
            ArrayList<Tuple> smallRelationTuples = memory.getTuples(0, smallRelation.getNumOfBlocks());
            ArrayList<Tuple> largeRelationTuples = block.getTuples();

            for(Tuple smallTuple:smallRelationTuples){
                for(Tuple largeTuple:largeRelationTuples){
                    if(smallRelation==TableOne){
                        tuples.add(combineTuple(newRelation, smallTuple, largeTuple));
                    }
                    else{
                        tuples.add(combineTuple(newRelation, largeTuple, smallTuple));
                    }
                }
            }
        }
        return tuples;
    }

    private static ArrayList<Tuple> nestedLoopJoin(SchemaManager schema_manager, MainMemory memory, String tableOne, String tableTwo){
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        Relation TableOne = schema_manager.getRelation(tableOne);
        Relation TableTwo = schema_manager.getRelation(tableTwo);
        int sizeOne = TableOne.getNumOfBlocks();
        int sizeTwo = TableTwo.getNumOfBlocks();
        Schema newSchema = combineSchema(schema_manager, tableOne, tableTwo);
        String newName="TempTable_"+tableOne+"_crossJoin_"+tableTwo;
        if(schema_manager.relationExists(newName)){
            schema_manager.deleteRelation(newName);
        }
        Relation newRelation = schema_manager.createRelation(newName, newSchema);

        for(int i=0; i<sizeOne; i++){
            TableOne.getBlock(i,0);
            Block blockOne = memory.getBlock(0);
            for(int j=0; j<sizeTwo; j++){
                TableTwo.getBlock(j,1);
                Block blockTwo = memory.getBlock(1);
                for(Tuple tupleOne:blockOne.getTuples()){
                    for(Tuple tupleTwo:blockTwo.getTuples()){
                        tuples.add(combineTuple( newRelation, tupleOne, tupleTwo));
                    }
                }
            }
        }
        return tuples;
    }

    @SuppressWarnings("Duplicates")
    private static Schema combineSchema(SchemaManager schemaManager, String tableOne, String tableTwo){
        ArrayList<String>combineFields = new ArrayList<String>();
        ArrayList<FieldType>combineTypes = new ArrayList<FieldType>();

        Relation TableOne = schemaManager.getRelation(tableOne);
        Relation TableTwo = schemaManager.getRelation(tableTwo);

        ArrayList<String>fieldsOne = TableOne.getSchema().getFieldNames();
        ArrayList<String>fieldsTwo = TableTwo.getSchema().getFieldNames();
        ArrayList<FieldType>typesOne = TableOne.getSchema().getFieldTypes();
        ArrayList<FieldType>typesTwo = TableTwo.getSchema().getFieldTypes();

        for(String field:fieldsOne){
            if(!field.contains("\\.")){
                String newName = tableOne+"."+field;
                combineFields.add(newName);
            }
            else{
                combineFields.add(field);
            }
        }

        for(String field:fieldsTwo){
            if(!field.contains("\\.")){
                String newName = tableTwo+"."+field;
                combineFields.add(newName);
            }
            else{
                combineFields.add(field);
            }
        }

        combineTypes.addAll(typesOne);
        combineTypes.addAll(typesTwo);

        Schema newSchema = new Schema(combineFields, combineTypes);
        return newSchema;
    }


    private static Tuple combineTuple( Relation relation, Tuple smalltuple, Tuple largetuple){
        Tuple result = relation.createTuple();
        int smallSize = smalltuple.getNumOfFields();
        int largeSize = largetuple.getNumOfFields();
        for(int i = 0; i<smallSize+largeSize; i++){
            if(i<smallSize){
                String temp = smalltuple.getField(i).toString();
                if(ExpressionTree.isInteger(temp)){
                    result.setField(i, Integer.parseInt(temp));
                }else{
                    result.setField(i, temp);
                }
            }else{
                String temp = largetuple.getField(i-smallSize).toString();
                if(ExpressionTree.isInteger(temp)){
                    result.setField(i, Integer.parseInt(temp));
                }else{
                    result.setField(i, temp);
                }
            }
        }
        return result;
    }



    private static Relation naturalJoin(SchemaManager schemaManager, MainMemory memory, String tableOne, String tableTwo, String JoinOn){
        ArrayList<Tuple> tuples;
        Relation r1 = schemaManager.getRelation(tableOne);
        Relation r2 = schemaManager.getRelation(tableTwo);
        int sizeOne = r1.getNumOfBlocks();
        int sizeTwo = r2.getNumOfBlocks();

        Relation smallRelation;
        if (sizeOne < sizeTwo) smallRelation = r1;
        else smallRelation = r2;

        if (smallRelation.getNumOfBlocks() < memory.getMemorySize()) {
            tuples = onePassNaturalJoin(schemaManager, memory, tableOne, tableTwo, JoinOn);
        }else{
            tuples = twoPassNaturalJoin(schemaManager, memory, tableOne, tableTwo, JoinOn);
        }

        String name = r1.getRelationName() + "_naturalJoin_" + r2.getRelationName();
        if(schemaManager.relationExists(name)) schemaManager.deleteRelation(name);
        Schema schema = combineSchema(schemaManager, tableOne, tableTwo);
        Relation tempRelation = schemaManager.createRelation(name, schema);

        int tupleNumber = tuples.size(), tuplesPerBlock = schema.getTuplesPerBlock(), tupleBlocks = 0;
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

    public static ArrayList<Tuple> onePassNaturalJoin(SchemaManager schemaManager, MainMemory memory, String tableOne, String tableTwo, String JoinOn){
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        Relation TableOne = schemaManager.getRelation(tableOne);
        Relation TableTwo = schemaManager.getRelation(tableTwo);
        int sizeOne = TableOne.getNumOfBlocks();
        int sizeTwo = TableTwo.getNumOfBlocks();

        Relation smallRelation, largeRelation;
        if (sizeOne < sizeTwo) {
            smallRelation = TableOne;
            largeRelation = TableTwo;
        }
        else {
            smallRelation = TableTwo;
            largeRelation = TableOne;
        }

        Schema newSchema = combineSchema(schemaManager, tableOne, tableTwo);
        String newName = "TempTable_"+tableOne+"_naturalJoin_"+tableTwo;
        if(schemaManager.relationExists(newName)){
            schemaManager.deleteRelation(newName);
        }
        Relation newRelation = schemaManager.createRelation(newName, newSchema);

        smallRelation.getBlocks(0,0,smallRelation.getNumOfBlocks());

        for(int i=0; i<largeRelation.getNumOfBlocks(); i++){
            largeRelation.getBlock(i, memory.getMemorySize()-1);
            Block block = memory.getBlock(memory.getMemorySize()-1);
            ArrayList<Tuple> smallRelationTuples = memory.getTuples(0, smallRelation.getNumOfBlocks());
            ArrayList<Tuple> largeRelationTuples = block.getTuples();

            for(Tuple largeTuple:largeRelationTuples){
                for(Tuple smallTuple:smallRelationTuples){
                    String result1 = smallTuple.getField(JoinOn).toString();
                    String result2 = largeTuple.getField(JoinOn).toString();
                    if(ExpressionTree.isInteger(result1) && ExpressionTree.isInteger(result2)){
                        if(Integer.parseInt(result1) == Integer.parseInt(result2)){
                            if(TableOne == smallRelation){
                                tuples.add(combineTuple(newRelation, smallTuple, largeTuple));
                            }else{
                                tuples.add(combineTuple(newRelation, largeTuple, smallTuple));
                            }
                        }else{
                            if(result1.equals(result2)){
                                if(TableOne == smallRelation){
                                    tuples.add(combineTuple(newRelation, smallTuple, largeTuple));
                                }else{
                                    tuples.add(combineTuple(newRelation, largeTuple, smallTuple));
                                }
                            }
                        }
                    }
                }
            }
        }
        return tuples;
    }

    public static ArrayList<Tuple> twoPassNaturalJoin(SchemaManager schemaManager, MainMemory memory, String tableOne, String tableTwo, String fieldName){
        //phase 1: making sorted sublists
        Relation t1 = schemaManager.getRelation(tableOne);
        Relation t2 = schemaManager.getRelation(tableTwo);
        QueryHelper.twoPassHelper(t1, memory, fieldName);
        QueryHelper.twoPassHelper(t2, memory, fieldName);

        //phase 2: merging and iterating
        Schema schema = combineSchema(schemaManager, tableOne, tableTwo);
        String name = tableOne + "_join_" + tableTwo;
        Relation crossRelation = schemaManager.createRelation(name, schema);

        ArrayList<Tuple> res = new ArrayList<>();
        int t1Blocks = t1.getNumOfBlocks(), t2Blocks = t2.getNumOfBlocks();
        int totalTuples = t1.getNumOfTuples() + t2.getNumOfTuples();
        ArrayList<ArrayList<Tuple>> tuples = new ArrayList<>();
        ArrayList<Pair<Integer, Integer>> t1BlockIndex = new ArrayList<>();
        ArrayList<Pair<Integer, Integer>> t2BlockIndex = new ArrayList<>();

        //bring in a block from each of the sorted sublists of each relation
        int i, j;
        for(i = 0, j = 0; i < t1Blocks; i += memory.getMemorySize(), j++){
            t1BlockIndex.add(new Pair<>(i + 1, Math.min(i + memory.getMemorySize(), t1Blocks)));
            t1.getBlock(i, j);
            tuples.add(memory.getTuples(j, 1));
        }
        int div = j;
        for(i = 0; i < t2Blocks; i += memory.getMemorySize(), j++){
            t2BlockIndex.add(new Pair<>(i + 1, Math.min(i + memory.getMemorySize(), t2Blocks)));
            t2.getBlock(i, j);
            tuples.add(memory.getTuples(j, 1));
        }

        for(int k = 0; k < totalTuples; ++k){
            for(i = 0; i < tuples.size(); ++i){
                //read in the next block from a sublist if its block is exhausted
                if(tuples.get(i).isEmpty()){
                    if(i < div){
                        if(t1BlockIndex.get(i).first < t1BlockIndex.get(i).second){
                            t1.getBlock(t1BlockIndex.get(i).first, i);
                            tuples.set(i, memory.getTuples(i, 1));
                            t1BlockIndex.get(i).first++;
                        }
                    }else{
                        if(t2BlockIndex.get(i - div).first < t2BlockIndex.get(i - div).second){
                            t2.getBlock(t2BlockIndex.get(i - div).first, i);
                            tuples.set(i, memory.getTuples(i, 1));
                            t2BlockIndex.get(i - div).first++;
                        }
                    }
                }
            }
            //System.out.println(tuples);
            //find the smallest key among the first remaining elements of all the sublists
            ArrayList<Tuple> t1MinTuples = new ArrayList<>();
            ArrayList<Tuple> t2MinTuples = new ArrayList<>();
            for(j = 0; j < tuples.size(); ++j){
                if(j < div){
                    if(!tuples.isEmpty() && !tuples.get(j).isEmpty()) t1MinTuples.add(tuples.get(j).get(0));
                }else{
                    if(!tuples.isEmpty() && !tuples.get(j).isEmpty()) t2MinTuples.add(tuples.get(j).get(0));
                }

            }
            Tuple t1MinTuple = t1.createTuple(), t2MinTuple = t2.createTuple();
            if(t1MinTuples.isEmpty() && t2MinTuples.isEmpty()) break;
            if(!t1MinTuples.isEmpty()) t1MinTuple = Collections.min(t1MinTuples, new TupleComparator(fieldName));
            if(!t2MinTuples.isEmpty()) t2MinTuple = Collections.min(t2MinTuples, new TupleComparator(fieldName));

            if(compareTuples(t1MinTuple, t2MinTuple, fieldName) == 0){
                ArrayList<Tuple> tmp1 = new ArrayList<>();
                ArrayList<Tuple> tmp2 = new ArrayList<>();
                //when t1min == t2min, we need to collect all t1min-tuples and t2min-tuples
                //read blocks from t1 and t2 until there are no such tuple in either relation
                //save found tuples in two Arraylists and remove found tuples
                //traverse temporary arrays to generate cross product tuples
                //flag to determine whether there are such tuples
                boolean flag = true;
                while(flag){
                    flag = false;
                    for(j = 0; j < tuples.size(); ++j){
                        if(j < div){
                            if(!tuples.get(j).isEmpty()){
                                if(compareTuples(tuples.get(j).get(0), t1MinTuple, fieldName) == 0){
                                    tmp1.add(tuples.get(j).get(0));
                                    tuples.get(j).remove(0);
                                    flag = true;
                                }
                            }else{
                                if(t1BlockIndex.get(j).first < t1BlockIndex.get(j).second){
                                    t1.getBlock(t1BlockIndex.get(j).first, j);
                                    tuples.set(j, memory.getTuples(j, 1));
                                    t1BlockIndex.get(j).first++;
                                    flag = true;
                                }else{
                                    flag = false;
                                }
                            }
                        }else{
                            if(!tuples.get(j).isEmpty()){
                                if(compareTuples(tuples.get(j).get(0), t2MinTuple, fieldName) == 0){
                                    tmp2.add(tuples.get(j).get(0));
                                    tuples.get(j).remove(0);
                                    flag = true;
                                }
                            }else{
                                if(t2BlockIndex.get(j - div).first < t2BlockIndex.get(j - div).second){
                                    t2.getBlock(t2BlockIndex.get(j - div).first, j);
                                    tuples.set(j, memory.getTuples(j, 1));
                                    t2BlockIndex.get(j - div).first++;
                                    flag = true;
                                }else{
                                    flag = false;
                                }
                            }
                        }
                    }
                }

                for(i = 0; i < tmp1.size(); ++i){
                    for(j = 0; j < tmp2.size(); ++j){
                        res.add(combineTuple(crossRelation, tmp1.get(i), tmp2.get(j)));
                    }
                }
            }else{
                //delete the smaller tuple
                Tuple minTuple;
                if(compareTuples(t1MinTuple, t2MinTuple, fieldName) > 0){
                    minTuple = t2MinTuple;
                }else{
                    minTuple = t1MinTuple;
                }
                for(j = 0; j < tuples.size(); ++j){
                    if(!tuples.get(j).isEmpty() && tuples.get(j).get(0).equals(minTuple)) tuples.get(j).remove(0);
                }
            }
        }

        //for(Tuple tuple : res) System.out.println(tuple);
        QueryHelper.clearMainMem(memory);
        return res;
    }


    private static int compareFields(Field a, Field b){
        if(a.type.equals(FieldType.INT)){
            return a.integer - b.integer;
        }else{
            return a.str.compareTo(b.str);
        }
    }

    private static int compareTuples(Tuple a, Tuple b, String fieldName){
        return compareFields(a.getField(fieldName), b.getField(fieldName));
    }
}



