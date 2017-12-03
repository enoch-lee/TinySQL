import java.io.*;
import java.util.ArrayList;
import java.nio.file.*;
import java.nio.charset.Charset;
import java.util.Scanner;

import storageManager.*;

public class Query {
    private static Parser parser = new Parser();
    private static MainMemory memory = new MainMemory();
    private static Disk disk = new Disk();
    private static SchemaManager schemaMG = new SchemaManager(memory, disk);
    private static String FILENAME;

    public Query(){ }

    private static void reset(){
        parser = new Parser();
        memory = new MainMemory();
        disk = new Disk();
        schemaMG = new SchemaManager(memory, disk);
        disk.resetDiskIOs();
        disk.resetDiskTimer();
    }

    public static void resetDisk(){
        disk.resetDiskIOs();
        disk.resetDiskTimer();
    }

    public static void parseQuery(String m){
        System.out.println(m);
        m = m.toLowerCase();
        String[] Command= m.trim().toLowerCase().split("[\\s]+");
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

    /************CREATE************/
    private static void createQuery(String sql){
        Statement stmt = parser.createStatement(sql);
        Schema schema = new Schema(stmt.fieldNames, stmt.fieldTypes);
        schemaMG.createRelation(stmt.tableName, schema);
    }

    /************DROP************/
    private static void dropQuery(String sql){
        Parser parser = new Parser();
        schemaMG.deleteRelation(parser.dropStatement(sql).trim());
    }

    /************DELETE************/
    //done: 1. optimize delete  2. filed size == 0  3. fill "holes"
    private static void deleteQuery(String sql){
        Statement stmt = parser.deleteStatement(sql);
        ParseTree parseTree = stmt.parseTree;
        String tableName = parseTree.tables.get(0);
        Relation relation = schemaMG.getRelation(tableName);
        int numOfBlocks = relation.getNumOfBlocks();
        int i = 0; //relation block index
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
        //System.out.println(relation);
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

    /************INSERT************/
    //done: 1. handle null 2. handle "E" 3. handle sub select
    private static void insertQuery(String sql){
        Statement stmt = parser.insertStatement(sql);
        Schema schema = schemaMG.getSchema(stmt.tableName);
        Relation relation = schemaMG.getRelation(stmt.tableName);
        if(stmt.subQuery == null){
            for(int i = 0; i < stmt.fieldValues.size(); ++i){
                Tuple tuple = relation.createTuple();
                for(int j = 0; j < stmt.fieldValues.get(0).size(); ++j ){
                    String value = stmt.fieldValues.get(i).get(j);
                    if(schema.getFieldType(stmt.fieldNames.get(j)) == FieldType.INT){
                        //handle NULL case
                        if(!value.equalsIgnoreCase("NULL")){
                            tuple.setField(stmt.fieldNames.get(j), Integer.parseInt(value));
                        }
                    }else{
                        tuple.setField(stmt.fieldNames.get(j), value);
                    }
                }
                appendTuple2Relation(relation, tuple);
            }
        }else{
            //handle case INSERT INTO course (sid, homework, project, exam, grade) SELECT * FROM course
            Statement subStmt = stmt.subQuery;
            ParseTree parseTree = subStmt.parseTree;
            Relation subRelation = schemaMG.getRelation(parseTree.tables.get(0));
            ArrayList<Tuple> tuples = new ArrayList<>();
            int numOfBlocks = subRelation.getNumOfBlocks(), memoryBlocks = memory.getMemorySize();
            if(numOfBlocks <= memoryBlocks){
                tuples = QueryHelper.selectQueryHelper(parseTree, subRelation, memory,0, numOfBlocks);
            }else{
                int remainNumber = numOfBlocks;
                int relationindex = 0;
                ArrayList<Tuple> tmp;
                while(remainNumber > memoryBlocks){
                    tmp = QueryHelper.selectQueryHelper(parseTree, subRelation, memory, relationindex, memoryBlocks);
                    tuples.addAll(tmp);
                    remainNumber = remainNumber - memoryBlocks;
                    relationindex = relationindex + memoryBlocks;
                }
                tmp = QueryHelper.selectQueryHelper(parseTree, subRelation, memory, relationindex, remainNumber);
                tuples.addAll(tmp);
            }
            for(Tuple tuple : tuples){
                appendTuple2Relation(relation, tuple);
            }
        }
    }

    /************SELECT************/
    private static void selectQuery(String sql){
        writeFile(sql, true);
        Query.writeFile("********START********", true);
        System.out.println("********START********");
        Statement stmt = parser.selectStatement(sql);
        ParseTree parseTree = stmt.parseTree;
        if(parseTree.tables.size() == 1){
            selectSingleRelation(parseTree);
        }else{
            selectMultiRelation(parseTree);
        }
        Query.writeFile("**********END**********", true);
        Query.writeFile("\r\n", true);
        System.out.println("**********END**********");
        System.out.println();
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
        Relation relation = null;
        ArrayList<String> tempRelations = new ArrayList<>();
        if(parseTree.tables.size() == 2) {
            if(parseTree.expressionTree != null && parseTree.expressionTree.natureJoin.size() > 0){
                Pair<ArrayList<String>, String> condition = parseTree.expressionTree.natureJoin.get(0);
                ArrayList<String> joinInfo = multipleSelectHelper(condition);
                relation = Join.naturalJoin(schemaMG, memory, joinInfo.get(0), joinInfo.get(1), joinInfo.get(2));
            }else {
                relation = Join.crossProduct(schemaMG, memory, parseTree.tables.get(0), parseTree.tables.get(1));
            }
        }else {
            if(parseTree.expressionTree != null && parseTree.expressionTree.natureJoin.size() !=0){
                for(int i=0; i<2; i++){
                    Pair<ArrayList<String>, String> condition = parseTree.expressionTree.natureJoin.get(i);
                    ArrayList<String> joinInfo = multipleSelectHelper(condition);
                    if(i==0) {
                        relation = Join.naturalJoin(schemaMG, memory, joinInfo.get(0), joinInfo.get(1), joinInfo.get(2));
                        // break;
                    }else{

                        relation = Join.crossProduct(schemaMG, memory, relation.getRelationName(), joinInfo.get(1));
                    }
                }
            }else{
                for(int i=0; i<parseTree.tables.size()-1; i++){
                    if(i==0){
                        relation = Join.crossProduct(schemaMG, memory, parseTree.tables.get(0), parseTree.tables.get(1));
                    }else{
                        relation = Join.crossProduct(schemaMG, memory, relation.getRelationName(), parseTree.tables.get(i+1));
                    }
                }
            }
        }
        tempRelations.add(relation.getRelationName());



        //if DISTINCT
        if(parseTree.distinct){
            relation = QueryHelper.distinct(schemaMG, relation, memory, parseTree.dist_attribute);
            QueryHelper.clearMainMem(memory);
            tempRelations.add(relation.getRelationName());
        }

        System.out.println(relation);

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
        //System.out.println(relation.getNumOfTuples());
        QueryHelper.project(relation, memory, parseTree);

        //delete all intermediate relations
        if(tempRelations.isEmpty()) return;
        for(String temp : tempRelations){
            if(schemaMG.relationExists(temp)) schemaMG.deleteRelation(temp);
        }
    }

    private static ArrayList<String> multipleSelectHelper(Pair<ArrayList<String>, String> condition){
        ArrayList<String>joinInfo = new ArrayList<String>();
        joinInfo.add(condition.first.get(0));
        joinInfo.add(condition.first.get(1));
        joinInfo.add(condition.second);
        return joinInfo;
    }

    private static void appendTuple2Relation(Relation relation, Tuple tuple){
        Block block;
        int memBlockIndex = 0;
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



    public static void setFileName(String name){
        FILENAME = name;
    }

    public static void writeFile(String output, Boolean newLine){
        try {
            Path file = Paths.get(FILENAME);
            if(newLine){
                output += "\r\n";
            }
            Files.write(file, output.getBytes(),  StandardOpenOption.APPEND);
        } catch(IOException ex) {
            ex.printStackTrace();
        }
    }

    public static void readFile(String fileName) {
        BufferedReader br;
        try {
            br = new BufferedReader(new FileReader(fileName));
            String line;
            while((line = br.readLine()) != null) {
                parseQuery(line);
            }
        } catch(IOException ex) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        long startTime = System.nanoTime();
        Query.reset();
        Query.setFileName("src/test2.txt");
        Query.readFile("src/test.txt");
//        Scanner in = new Scanner(System.in);
//        System.out.println("Set Up Output File Name:");
//        String outputFile = in.nextLine();
//        Query.setFileName("src/" + outputFile);
//        while (true) {
//            System.out.println("Select TinySQL Mode (Press 1 or 2 to Select and Press Q to quit):");
//            System.out.println("1.Input SQL Statement");
//            System.out.println("2.Input SQL File Name");
//            String s = in.nextLine();
//            if(s.equalsIgnoreCase("1")) {
//                System.out.println("Please Input SQL Query at a Line:");
//                String sql = in.nextLine();
//                if(sql.equalsIgnoreCase("q")) break;
//                Query.parseQuery(sql);
//            } else if (s.equalsIgnoreCase("2")) {
//                System.out.println("Please Input a File Name(.txt):");
//                String fileName = in.nextLine();
//                if(fileName.equalsIgnoreCase("q")) break;
//                Query.readFile("src/" + fileName);
//            }else if(s.equalsIgnoreCase("Q")){
//                break;
//            }else{
//                System.out.println("Invalid Input!");
//            }
//        }
        //Query.readFile("src/test.txt");
        long endTime = System.nanoTime();
        System.out.println("Used: " + (endTime - startTime) / 1000000000 + "s");
    }
}


