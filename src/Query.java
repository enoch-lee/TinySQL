import java.io.IOException;
import java.util.ArrayList;
import storageManager.*;

public class Query {
    private static Parser parser = new Parser();
    private static MainMemory memory = new MainMemory();
    private static Disk disk = new Disk();
    private static SchemaManager schemaMG = new SchemaManager(memory, disk);

    public SchemaManager getSchemaMG() {
        return schemaMG;
    }

    public Query(){ }

    private static void reset(){
        parser = new Parser();
        memory = new MainMemory();
        disk = new Disk();
        schemaMG = new SchemaManager(memory, disk);
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
        ParseTree parseTree = stmt.parseTree;
        if(parseTree.tables.size() == 1){
            selectSingleRelation(parseTree);
        }else{
            selectMultiRelation(parseTree);
        }
    }

    private static void selectSingleRelation(ParseTree parseTree){
        String tableName = parseTree.tables.get(0);
        Relation relation = schemaMG.getRelation(tableName);
        ArrayList<String> tempRelations = new ArrayList<>();

        //if DISTINCT
        if(parseTree.distinct){
            relation = QueryHelper.distinct(schemaMG, relation, memory, parseTree.dist_attribute);
            QueryHelper.clearMainMem(memory);
            tempRelations.add(relation.getRelationName());
        }

        //selection
        if(parseTree.where){
            relation = QueryHelper.select(schemaMG, relation, memory, parseTree);
            QueryHelper.clearMainMem(memory);
            tempRelations.add(relation.getRelationName());
        }

        //if ORDER BY
        if(parseTree.order){
            relation = QueryHelper.sort(schemaMG, relation, memory, parseTree.orderBy);
            QueryHelper.clearMainMem(memory);
            tempRelations.add(relation.getRelationName());
        }

        //projection
        QueryHelper.project(relation, memory, parseTree);
        QueryHelper.clearMainMem(memory);

        //delete all intermediate relations
        if(tempRelations.isEmpty()) return;
        for(String temp : tempRelations){
            if(schemaMG.relationExists(temp)) schemaMG.deleteRelation(temp);
        }
    }


    private static void selectMultiRelation(ParseTree parseTree){
        String t1 = parseTree.tables.get(0);
        String t2 = parseTree.tables.get(1);
        Relation r1 = schemaMG.getRelation(t1);
        Relation r2 = schemaMG.getRelation(t2);
        Relation relation;
        ArrayList<String> tempRelations = new ArrayList<>();

        //if DISTINCT
        if(parseTree.distinct){
            r1 = QueryHelper.distinct(schemaMG, r1, memory, "sid");
            r2 = QueryHelper.distinct(schemaMG, r2, memory, "sid");
        }

        //selection
        if(parseTree.where){
            relation = Join.naturalJoin(schemaMG, memory, t1, t2, "sid");
            //System.out.println(relation);
            //relation = QueryHelper.select(schemaMG, relation, memory, parseTree);
        }else{
            relation = Join.crossProduct(schemaMG, memory, t1, t2);
        }
        QueryHelper.clearMainMem(memory);
        tempRelations.add(relation.getRelationName());

        //if ORDER BY
        if(parseTree.order){
            relation = QueryHelper.sort(schemaMG, relation, memory, parseTree.orderBy);
            QueryHelper.clearMainMem(memory);
            tempRelations.add(relation.getRelationName());
        }

        //projection
        QueryHelper.project(relation, memory, parseTree);
        schemaMG.deleteRelation(relation.getRelationName());

        //delete all intermediate relations
        if(tempRelations.isEmpty()) return;
        for(String temp : tempRelations){
            if(schemaMG.relationExists(temp)) schemaMG.deleteRelation(temp);
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

    //done: 1. handle null 2. handle "E"
    //todo: "NULL"
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
                    if(!value.equals("NULL")){
                        tuple.setField(stmt.fieldNames.get(j), Integer.parseInt(value));
                    }
                }else{
                    tuple.setField(stmt.fieldNames.get(j), value);
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

    public static void main(String[] args) throws IOException {
        Query.reset();
        Query.parseQuery("CREATE TABLE course (sid INT, homework INT, project INT, exam INT, grade STR20)");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (12, 99, 100, 100, \"A\")");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (12, 100, 100, 98, \"C\")");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 100, 50, 90, \"E\")");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 100, 100, 100, \"A\")");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 100, 100, 66, \"A\")");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 50, 50, 61, \"D\")");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 0, 0, 0, \"E\")");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 0, 0, 0, \"E\")");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 50, 50, 59, \"D\")");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 50, 50, 56, \"D\")");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 0, 0, 0, \"E\")");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (7, 100, 100, 66, \"A\"");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (8, 100, 100, 66, \"A\"");
//        Query.parseQuery("SELECT sid, grade FROM course WHERE sid > 5 ORDER BY grade");

        Query.parseQuery("CREATE TABLE course2 (sid INT, exam INT, grade STR20)");
        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (3, 100, \"A\")");
        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (12, 101, \"A\")");
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
        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (99, 100, \"A\")");
        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (99, 25, \"E\")");
        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (99, 100, \"A\")");
        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (99, 100, \"A\")");
        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (99, 25, \"E\")");
        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (99, 100, \"A\")");
        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (99, 25, \"E\")");
        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (99, 100, \"A\")");
        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (99, 100, \"A\")");
        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (99, 25, \"E\")");
        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (99, 100, \"A\")");
        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (99, 25, \"E\")");

        //Query.parseQuery("SELECT DISTINCT course.sid, course2.sid FROM course, course2 where course.sid = course2.sid");
          Query.parseQuery("SELECT course.sid FROM course");
        //String test = "course.sid";
//        Relation relation = Query.crossJoin("course", "course2");
//        ArrayList<Tuple> tuples = Query.twoPassNaturalJoin("course", "course2", "sid");
//        for(Tuple tuple : tuples) System.out.println(tuple);
    }
}


