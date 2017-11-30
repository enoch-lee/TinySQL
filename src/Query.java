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

    public Query(){
        parser = new Parser();
        memory = new MainMemory();
        disk = new Disk();
        schemaMG = new SchemaManager(memory, disk);
        helper = new QueryHelper();
        disk.resetDiskIOs();
        disk.resetDiskTimer();
    }

    public void parseQuery(String m){
        String[] Command= m.trim().toLowerCase().split("\\s");
        String first = Command[0];
        switch (first){
            case "create": this.createQuery(m);
                break;
            case "select": this.selectQuery(m);
                break;
            case "drop"  : this.dropQuery(m);
                break;
            case "delete": this.deleteQuery(m);
                break;
            case "insert": this.insertQuery(m);
                break;
            default: System.out.println("Not a legal command!");
        }
    }

    private void createQuery(String sql){
        Statement stmt = parser.createStatement(sql);
        Schema schema = new Schema(stmt.fieldNames, stmt.fieldTypes);
        schemaMG.createRelation(stmt.tableName, schema);
    }

    private void selectQuery(String sql){
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
    private ArrayList<Tuple> selectQueryHelper(ParseTree parseTree, Relation relation, int relationIndex, int memoryIndex, int loop ){
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
    private void sortTuples(ArrayList<Tuple> tuples, String fieldName){
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

    private void dropQuery(String sql){
        Parser parser = new Parser();
        schemaMG.deleteRelation(parser.dropStatement(sql).trim());
    }

    //done: 1. optimize delete  2. filed size == 0  3. fill "holes"
    private void deleteQuery(String sql){
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

    private void deleteQueryHelper(Relation relation, MainMemory memory, ParseTree parseTree, int relation_block_index, int num_blocks){
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
        clearMainMem(memory);
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
    private void insertQuery(String sql){
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

    private void clearMainMem(MainMemory memory){
        for(int i = 0; i < memory.getMemorySize(); ++i) memory.getBlock(i).clear();
    }

    public static void main(String[] args) throws IOException {
        Query q = new Query();
//        q.parseQuery("create table a (c1 int, c2 str20)");
//        q.parseQuery("create table b (c1 int, c2 int, c3 str20)");
//        q.parseQuery("insert into a (c1, c2) values (6, \"c\")");
//        q.parseQuery("insert into a (c1, c2) values (10, \"a\")");
//        q.sort("a", "c1");
//        q.parseQuery("insert into a (c1, c2) values (20, 10)");
//        q.parseQuery("insert into a (c1, c2) values (30, 10)");
//        q.parseQuery("insert into b (c1, c2, c3) values (20, 10, \"b\")");
//        q.parseQuery("insert into b (c1, c2, c3) values (10, 20, c)");
//        q.parseQuery("insert into table_name (c1, c2, c3) values (5, 30, test3)");
//        q.parseQuery("drop table table_name");

        q.parseQuery("create table a (c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, c7 int, c8 int)");
        q.parseQuery("insert into a (c1, c2, c3, c4, c5, c6, c7, c8) values (11, 2, 3, 4, 5, 6, 7, 8)");
        q.parseQuery("insert into a (c1, c2, c3, c4, c5, c6, c7, c8) values (1, 2, 3, 4, 5, 6, 7, 8)");
        q.parseQuery("insert into a (c1, c2, c3, c4, c5, c6, c7, c8) values (3, 2, 3, 4, 5, 6, 7, 8)");
        q.parseQuery("insert into a (c1, c2, c3, c4, c5, c6, c7, c8) values (4, 2, 3, 4, 5, 6, 7, 8)");
        q.parseQuery("insert into a (c1, c2, c3, c4, c5, c6, c7, c8) values (5, 2, 3, 4, 5, 6, 7, 8)");
        q.parseQuery("insert into a (c1, c2, c3, c4, c5, c6, c7, c8) values (6, 2, 3, 4, 5, 6, 7, 8)");
        q.parseQuery("insert into a (c1, c2, c3, c4, c5, c6, c7, c8) values (7, 2, 3, 4, 5, 6, 7, 8)");
        q.parseQuery("insert into a (c1, c2, c3, c4, c5, c6, c7, c8) values (8, 2, 3, 4, 5, 6, 7, 8)");
        q.parseQuery("insert into a (c1, c2, c3, c4, c5, c6, c7, c8) values (9, 2, 3, 4, 5, 6, 7, 8)");
        q.parseQuery("insert into a (c1, c2, c3, c4, c5, c6, c7, c8) values (10, 2, 3, 4, 5, 6, 7, 8)");
        q.parseQuery("insert into a (c1, c2, c3, c4, c5, c6, c7, c8) values (12, 2, 3, 4, 5, 6, 7, 8)");
        q.parseQuery("insert into a (c1, c2, c3, c4, c5, c6, c7, c8) values (1, 2, 3, 4, 5, 6, 7, 8)");
        q.parseQuery("delete from a where c1 = 5 or c1 = 1");
        q.parseQuery("select * from a");
//        q.sort("a", "c1");

//        q.parseQuery("create table a (c1 int, c2 int, c3 int, c4 int)");
//        q.parseQuery("insert into a (c1, c2, c3, c4) values (11, 2, 3, 4)");
//        q.parseQuery("insert into a (c1, c2, c3, c4) values (2, 2, 3, 4)");
//        q.parseQuery("insert into a (c1, c2, c3, c4) values (3, 2, 3, 4)");
//        q.parseQuery("insert into a (c1, c2, c3, c4) values (4, 2, 3, 4)");
//        q.parseQuery("insert into a (c1, c2, c3, c4) values (5, 2, 3, 4)");
//        q.parseQuery("insert into a (c1, c2, c3, c4) values (6, 2, 3, 4)");
//        q.parseQuery("insert into a (c1, c2, c3, c4) values (7, 2, 3, 4)");
//        q.parseQuery("insert into a (c1, c2, c3, c4) values (8, 2, 3, 4)");
//        q.parseQuery("insert into a (c1, c2, c3, c4) values (9, 2, 3, 4)");
//        q.parseQuery("insert into a (c1, c2, c3, c4) values (10, 2, 3, 4)");
//        q.parseQuery("insert into a (c1, c2, c3, c4) values (1, 2, 3, 4)");
//        q.parseQuery("insert into a (c1, c2, c3, c4) values (12, 2, 3, 4)");
//        q.parseQuery("insert into a (c1, c2, c3, c4) values (11, 2, 3, 4)");
//        q.parseQuery("insert into a (c1, c2, c3, c4) values (2, 2, 3, 4)");
//        q.parseQuery("insert into a (c1, c2, c3, c4) values (3, 2, 3, 4)");
//        q.parseQuery("insert into a (c1, c2, c3, c4) values (4, 2, 3, 4)");
//        q.parseQuery("insert into a (c1, c2, c3, c4) values (5, 2, 3, 4)");
//        q.parseQuery("insert into a (c1, c2, c3, c4) values (6, 2, 3, 4)");
//        q.parseQuery("insert into a (c1, c2, c3, c4) values (7, 2, 3, 4)");
//        q.parseQuery("insert into a (c1, c2, c3, c4) values (8, 2, 3, 4)");
//        q.parseQuery("insert into a (c1, c2, c3, c4) values (9, 2, 3, 4)");
//        q.parseQuery("insert into a (c1, c2, c3, c4) values (0, 2, 3, 4)");
//        q.parseQuery("insert into a (c1, c2, c3, c4) values (1, 2, 3, 4)");


        //q.sort("a", "c1");
//        q.parseQuery("delete from a where c1 = 10 or c1 = 3 or c1 = 5");
//        q.parseQuery("select c1, c2 from a");
//        System.out.println(disk.getDiskIOs());
//        System.out.println(disk.getDiskTimer());
    }
}


