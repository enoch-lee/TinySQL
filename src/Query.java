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

    public Query(){
        parser = new Parser();
        memory = new MainMemory();
        disk = new Disk();
        schemaMG = new SchemaManager(memory, disk);
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
        //Schema schema = schemaMG.getSchema(tableName);
        Relation relation = schemaMG.getRelation(tableName);
        Block block;
        for(int i = 0; i < relation.getNumOfBlocks(); ++i){
            relation.getBlock(i, 0);
            block  = memory.getBlock(0);
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
            rmDupTuples(res, parseTree.dist_attribute);
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
                    System.out.print(tuple.getField(attr) + " ");
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

    private void rmDupTuples(ArrayList<Tuple> tuples, String fieldName){
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

    private void deleteQuery(String sql){
        Statement stmt = parser.deleteStatement(sql);
        ParseTree parseTree = stmt.parseTree;
        String tableName = parseTree.tables.get(0);
        Relation relation = schemaMG.getRelation(tableName);
        Block block;
        for(int i = 0; i < relation.getNumOfBlocks(); ++i){
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

    private void insertQuery(String sql){
        Statement stmt = parser.insertStatement(sql);
        Schema schema = schemaMG.getSchema(stmt.tableName);
        Relation relation = schemaMG.getRelation(stmt.tableName);
        //System.out.println(stmt.fieldValues.get(0).size());
        for(int i = 0; i < stmt.fieldValues.size(); ++i){
            Tuple tuple = relation.createTuple();
            for(int j = 0; j < stmt.fieldValues.get(0).size(); ++j ){
                String value = stmt.fieldValues.get(i).get(j);
                if(schema.getFieldType(j) == FieldType.INT){
                    tuple.setField(j, Integer.parseInt(value));
                }else{
                    tuple.setField(j, value);
                }
            }
            //System.out.println(tuple.getField(0).integer);
            appendTuple2Relation(relation, tuple, 0);
        }
        //System.out.println(relation.getNumOfTuples());
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
            block.appendTuple(tuple);
            relation.setBlock(0, memBlockIndex);
        }
    }

    public static void main(String[] args) throws IOException {
        Query q = new Query();
        q.parseQuery("create table table_name (c1 int, c2 int, c3 str20)");
        q.parseQuery("insert into table_name (c1, c2, c3) values (10, 10, test1)");
        q.parseQuery("insert into table_name (c1, c2, c3) values (10, 20, test2)");
        q.parseQuery("insert into table_name (c1, c2, c3) values (5, 30, test3)");
        q.parseQuery("select distinct c1 from table_name");
        q.parseQuery("drop table table_name");
    }
}
