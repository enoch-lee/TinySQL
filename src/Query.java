import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.PriorityQueue;

import storageManager.*;

public class Query {
    private static Parser parser;
    private static MainMemory memory;
    private static Disk disk;
    private static SchemaManager schemaMG;
    private static QueryHelper helper;

    public SchemaManager getSchemaMG() {
        return schemaMG;
    }

    public Query(){
        parser = new Parser();
        memory = new MainMemory();
        disk = new Disk();
        schemaMG = new SchemaManager(memory, disk);
        helper = new QueryHelper();
        disk.resetDiskIOs();
        disk.resetDiskTimer();
    }

    private static void reset(){
        disk.resetDiskIOs();
        disk.resetDiskTimer();
    }

    public static void parseQuery(String m){
        m = m.toLowerCase();
        String[] Command= m.trim().toLowerCase().split("\\s");
        String first = Command[0];
        switch (first){
            case "create": createQuery(m);
                break;
            case "select": selectQuery(m);
                break;
            case "drop"  : dropQuery(m);
                break;
            case "delete": deleteQuery(m);
                break;
            case "insert": insertQuery(m);
                break;
            default: System.out.println("Not a legal command!");
        }
    }

    private static void createQuery(String sql){
        Statement stmt = parser.createStatement(sql);
        Schema schema = new Schema(stmt.fieldNames, stmt.fieldTypes);
        schemaMG.createRelation(stmt.tableName, schema);
    }

    private static void selectQuery(String sql){
        Statement stmt = parser.selectStatement(sql);
        ArrayList<Tuple> res = new ArrayList<>();
        ParseTree parseTree = stmt.parseTree;
        String tableName = parseTree.tables.get(0);
        Relation relation = schemaMG.getRelation(tableName);
        int RelationNumber = relation.getNumOfBlocks();
        int MemoryNumber = memory.getMemorySize();

        if(RelationNumber <= MemoryNumber){
            res=selectQueryHelper(parseTree, relation, 0, 0,RelationNumber);
        }else{
            int remainNumber = RelationNumber;
            int relationindex = 0;
            while(remainNumber > MemoryNumber){
                ArrayList<Tuple> result=selectQueryHelper(parseTree, relation, relationindex,0,MemoryNumber);
                res.addAll(result);
                remainNumber = remainNumber - MemoryNumber;
                relationindex = relationindex + MemoryNumber;
            }
            ArrayList<Tuple> result=selectQueryHelper(parseTree, relation, relationindex, 0, remainNumber);
            res.addAll(result);
        }

        //if DISTINCT
        if(parseTree.distinct){
            if(relation.getNumOfBlocks() > memory.getMemorySize()){
                res = QueryHelper.twoPassRemoveDuplicate(relation, memory, parseTree.dist_attribute);
            }else{
                res = QueryHelper.onePassRemoveDuplicate(relation, memory, parseTree.dist_attribute);
            }

        }
        //if ORDER BY
        if(parseTree.order){
            sortTuples(res, parseTree.orderBy);
        }

        //print output tuples
        for(Tuple tuple : res){
            if(parseTree.attributes.get(0).equals("*")){
                System.out.println(tuple);
            }else{
                for(String attr : parseTree.attributes){
                    if(!tuple.isNull()) System.out.print(tuple.getField(attr) + " ");
                }
                System.out.println();
            }
        }
    }

    //get blocks once to reduce disk timer
    private static ArrayList<Tuple> selectQueryHelper(ParseTree parseTree, Relation relation, int relationIndex, int memoryIndex, int loop ){
        Block block;
        ArrayList<Tuple> res = new ArrayList<>();
        relation.getBlocks(relationIndex, memoryIndex, loop);
        for(int i=0; i<loop; i++){
            block =memory.getBlock(i);
            ArrayList<Tuple> tuples = block.getTuples();
            for(Tuple tuple : tuples){
                if(parseTree.where){
                    parseTree.expressionTree.checkTuple(tuple);
                }else{
                    //System.out.println(tuple);
                    res.add(tuple);
                }
            }
        }
        return res;
    }

    //not optimized
    private void _selectQuery(String sql){
        Statement stmt = parser.selectStatement(sql);
        ArrayList<Tuple> res = new ArrayList<>();
        ParseTree parseTree = stmt.parseTree;
        String tableName = parseTree.tables.get(0);
        //Schema schema = schemaMG.getSchema(tableName);
        Relation relation = schemaMG.getRelation(tableName);
        Block block;
        //System.out.println(relation.getNumOfBlocks());
        for(int i = 0; i < relation.getNumOfBlocks(); ++i){
            relation.getBlock(i, 0);
            block  = memory.getBlock(0);
            //System.out.println(block.getNumTuples());
            ArrayList<Tuple> tuples = block.getTuples();
            for(Tuple tuple : tuples){
                if(parseTree.where){
                    if(parseTree.expressionTree.checkTuple(tuple)){
                        res.add(tuple);
                    }
                }else{
                    res.add(tuple);
                }
            }
        }
        //if DISTINCT
        if(parseTree.distinct){
            rmDuplicateTuples(res, parseTree.dist_attribute);
        }
        //if ORDER BY
        if(parseTree.order){
            sortTuples(res, parseTree.orderBy);
        }

        //print output tuples
        //System.out.println(res.size());
        for(Tuple tuple : res){
            if(parseTree.attributes.get(0).equals("*")){
                System.out.println(tuple);
            }else{
                for(String attr : parseTree.attributes){
                    if(!tuple.isNull()) System.out.print(tuple.getField(attr) + " ");
                }
                System.out.println();
            }
        }
    }
    private static void sortTuples(ArrayList<Tuple> tuples, String fieldName){
        ArrayList<Tuple> tmp = new ArrayList<>();
        PriorityQueue<Tuple> pq = new PriorityQueue<Tuple>(new Comparator<Tuple>() {
            @Override
            //desc order
            public int compare(Tuple t1, Tuple t2) {
                if(t1.getField(fieldName).type.equals(FieldType.STR20)){
                    return t1.getField(fieldName).str.compareTo(t2.getField(fieldName).str);
                }else{
                    return t1.getField(fieldName).integer > t2.getField(fieldName).integer ? 1 : -1;
                }
            }
        });
        pq.addAll(tuples);
        tuples.clear();
        while(!pq.isEmpty()){
            tuples.add(pq.poll());
        }
    }
    private void rmDuplicateTuples(ArrayList<Tuple> tuples, String fieldName){
        ArrayList<Tuple> tmp = new ArrayList<>(tuples);
        tuples.clear();
        if(tmp.get(0).getField(fieldName).type.equals(FieldType.INT)){
            HashSet<Integer> hash = new HashSet<>();
            for(Tuple tuple : tmp){
                if(hash.add(tuple.getField(fieldName).integer)) {
                    tuples.add(tuple);
                }
            }
        }else{
            HashSet<String> hash = new HashSet<>();
            for(Tuple tuple : tmp){
                if(hash.add(tuple.getField(fieldName).str)) {
                    tuples.add(tuple);
                }
            }
        }
    }

    private static void dropQuery(String sql){
        Parser parser = new Parser();
        schemaMG.deleteRelation(parser.dropStatement(sql).trim());
    }

    //done: 1. optimize delete  2. filed size == 0  3. fill "holes"
    private static void deleteQuery(String sql){
        Statement stmt = parser.deleteStatement(sql);
        ParseTree parseTree = stmt.parseTree;
        String tableName = parseTree.tables.get(0);
        Relation relation = schemaMG.getRelation(tableName);
        Block block;
        int numOfBlocks = relation.getNumOfBlocks();
        //relation block index
        int i = 0;
        while(i < numOfBlocks){
            if(numOfBlocks - i <= 10){
                relation.getBlocks(i, 0, numOfBlocks - i);
                deleteQueryHelper(relation, memory, parseTree, i, numOfBlocks - i);
                break;
            }else{
                relation.getBlocks(i, 0, memory.getMemorySize());
                deleteQueryHelper(relation, memory, parseTree, i, memory.getMemorySize());
                i += 10;
            }
        }
        QueryHelper.fillHoles(relation, memory);
    }

    private static void deleteQueryHelper(Relation relation, MainMemory memory, ParseTree parseTree, int relation_block_index, int num_blocks){
        Block block;
        for(int i = 0; i < num_blocks; ++i){
            block = memory.getBlock(i);
            if(!block.isEmpty()){
                ArrayList<Tuple> tuples = block.getTuples();
                if(parseTree.where){
                    for(int j = 0; j < tuples.size(); ++j){
                        if(parseTree.expressionTree.checkTuple(tuples.get(j))){
                            block.invalidateTuple(j);
                        }
                    }
                }else{
                    block.invalidateTuples();
                }
            }
        }
        relation.setBlocks(relation_block_index, 0, num_blocks);
        QueryHelper.clearMainMem(memory);
    }

    //not optimized
    private void _deleteQuery(String sql){
        Statement stmt = parser.deleteStatement(sql);
        ParseTree parseTree = stmt.parseTree;
        String tableName = parseTree.tables.get(0);
        Relation relation = schemaMG.getRelation(tableName);
        Block block;
        int numOfBlocks = relation.getNumOfBlocks();
        for(int i = 0; i < numOfBlocks; ++i){
            relation.getBlock(i, 0);
            block  = memory.getBlock(0);
            if(parseTree.where){
                ArrayList<Tuple> tuples = block.getTuples();
                for(int j = 0; j < tuples.size(); ++j){
                    if(parseTree.expressionTree.checkTuple(tuples.get(j))){
                        block.invalidateTuple(j);
                    }
                }
            }else{
                block.invalidateTuples();
            }
            relation.setBlock(i, 0);
        }
    }

    //done: 1. handle null 2. handle "E"
    private static void insertQuery(String sql){
        Statement stmt = parser.insertStatement(sql);
        Schema schema = schemaMG.getSchema(stmt.tableName);
        Relation relation = schemaMG.getRelation(stmt.tableName);
        for(int i = 0; i < stmt.fieldValues.size(); ++i){
            Tuple tuple = relation.createTuple();
            for(int j = 0; j < stmt.fieldValues.get(0).size(); ++j ){
                String value = stmt.fieldValues.get(i).get(j);
                if(schema.getFieldType(j) == FieldType.INT){
                    //handle NULL case
                    if(value.equals("NULL")){
                        tuple.setField(stmt.fieldNames.get(j), "NULL");
                    }else{
                        tuple.setField(stmt.fieldNames.get(j), Integer.parseInt(value));
                    }
                }else{
                    if(value.equals("NULL")){
                        tuple.setField(stmt.fieldNames.get(j), "NULL");
                    }else{
                        tuple.setField(stmt.fieldNames.get(j), value);
                    }
                }
            }
            appendTuple2Relation(relation, tuple, 0);
        }
    }

    private static void appendTuple2Relation(Relation relation, Tuple tuple, int memBlockIndex){
        Block block;
        if(relation.getNumOfBlocks() != 0){
            relation.getBlock(relation.getNumOfBlocks() - 1, memBlockIndex);
            block = memory.getBlock(memBlockIndex);
            if(block.isFull()){
                block.clear();
                block.appendTuple(tuple);
                relation.setBlock(relation.getNumOfBlocks(), memBlockIndex);
            }else{
                block.appendTuple(tuple);
                relation.setBlock(relation.getNumOfBlocks() - 1, memBlockIndex);
            }
        }else{
            block = memory.getBlock(memBlockIndex);
            //be sure to clear the main memory block
            block.clear();
            block.appendTuple(tuple);
            relation.setBlock(0, memBlockIndex);
        }
    }
    public static Relation crossJoin(String tableOne, String tableTwo){
        return Join.crossJoin(schemaMG, memory, tableOne, tableTwo);
    }
    public static ArrayList<Tuple> onePassNaturalJoin(String tableOne, String tableTwo, String fieldName){
        return Join.onePassNaturalJoin(schemaMG, memory, tableOne, tableTwo, fieldName);
    }
    public static ArrayList<Tuple> twoPassNaturalJoin(String tableOne, String tableTwo, String fieldName){
        return Join.twoPassNaturalJoin(schemaMG, memory, tableOne, tableTwo, fieldName);
    }

    public static void main(String[] args) throws IOException {
        Query q = new Query();
        Query.parseQuery("CREATE TABLE course (sid INT, homework INT, project INT, exam INT, grade STR20)");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 99, 100, 100, \"A\")");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 100, 100, 98, \"C\")");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (3, 100, 50, 90, \"E\")");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (2, 100, 100, 100, \"A\")");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (9, 100, 100, 66, \"A\")");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (6, 50, 50, 61, \"D\")");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (7, 0, 0, 0, \"E\")");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (8, 0, 0, 0, \"E\")");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (5, 50, 50, 59, \"D\")");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (10, 50, 50, 56, \"D\")");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (11, 0, 0, 0, \"E\")");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (12, 100, 100, 66, \"A\"");

        Query.parseQuery("CREATE TABLE course2 (sid INT, exam INT, grade STR20)");
        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (1, 100, \"A\")");
        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (1, 101, \"A\")");
        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (12, 25, \"E\")");
        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (99, 100, \"A\")");
        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (99, 25, \"E\")");
        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (99, 100, \"A\")");
        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (99, 25, \"E\")");
        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (99, 100, \"A\")");
        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (99, 25, \"E\")");
        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (99, 100, \"A\")");
        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (99, 25, \"E\")");
        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (99, 100, \"A\")");
        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (1, 25, \"E\")");
//        Relation relation = Query.crossJoin("course", "course2");
        ArrayList<Tuple> tuples = Query.twoPassNaturalJoin("course", "course2", "sid");
        for(Tuple tuple : tuples) System.out.println(tuple);
    }
}


