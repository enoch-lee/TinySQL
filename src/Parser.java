import storageManager.FieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Parser {

    public Statement createStatement(String sql) {
        Statement stmt = new Statement();
        String regex = "create[\\s]+table[\\s]+(.+)[\\s]+\\((.*)\\)";
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(sql);
        while(m.find()){
            stmt.tableName = m.group(1);
            String[] tmp = m.group(2).split("[\\s]*,[\\s]*");
            for(String s : tmp){
                stmt.fieldNames.add(s.split(" ")[0]);
                if(s.split("[\\s]+")[1].equals("int")){
                    stmt.fieldTypes.add(FieldType.INT);
                }else{
                    stmt.fieldTypes.add(FieldType.STR20);
                }
            }
        }
        return stmt;
    }

    public String dropStatement(String sql){
        return sql.split("[\\s]+")[2];
    }

    public Statement selectStatement(String m){
        String[] Command= m.trim().toLowerCase().replaceAll("[,\\s]+", " ").split("\\s");

        Statement stmt = new Statement();
        ParseTree parseTree = new ParseTree("select");

        for(int i=0; i<Command.length;i++) {
            String word=Command[i];
            //System.out.print(word);
            if (word.equals("distinct")) {
                parseTree.distinct=true;
                parseTree.distID=i;
            }
            if (word.equals("from")){
                parseTree.fromID=i;
            }
            if (word.equals("where")){
                parseTree.whereID=i;
                parseTree.where=true;
            }
            if(i<=Command.length-2){
                if (word.equals("order")&&Command[i+1].equals("by")){
                    parseTree.order=true;
                    parseTree.orderID=i;
                }
            }
        }
        if(parseTree.distinct) {
            parseTree.dist_attribute = Command[parseTree.distID + 1];
            parseTree.attributes = new ArrayList<String>(Arrays.asList(Arrays.copyOfRange(Command, 2, parseTree.fromID)));
        }else{
            parseTree.attributes =  new ArrayList<String>(Arrays.asList(Arrays.copyOfRange(Command, 1, parseTree.fromID)));
        }
        if(parseTree.where){
            parseTree.tables= new ArrayList<String>(Arrays.asList(Arrays.copyOfRange(Command, parseTree.fromID+1, parseTree.whereID)));
            if(parseTree.order){
                String[] condition= Arrays.copyOfRange(Command, parseTree.whereID+1, parseTree.orderID);
                parseTree.expressionTree = new ExpressionTree(condition);
                parseTree.expressionTree.PrintTreeNode();
                parseTree.orderBy=Command[Command.length-1];
            }else{
                String[] condition=Arrays.copyOfRange(Command, parseTree.whereID+1,Command.length);
                parseTree.expressionTree= new ExpressionTree(condition);
                parseTree.expressionTree.PrintTreeNode();
            }
        }else{
            if(parseTree.order){
                parseTree.tables= new ArrayList<String>(Arrays.asList(Arrays.copyOfRange(Command,parseTree.fromID+1, parseTree.orderID)));
            }else{
                parseTree.tables= new ArrayList<String>(Arrays.asList(Arrays.copyOfRange(Command, parseTree.fromID+1, Command.length)));
            }
        }

        stmt.parseTree = parseTree;
        return stmt;
    }

    public Statement deleteStatement(String m){
        String[] Command= m.trim().toLowerCase().replaceAll("[,\\s]+", " ").split("\\s");
        ParseTree parseTree = new ParseTree("delete");
        Statement stmt = new Statement();

        for(int i=0; i<Command.length;i++) {
            String word=Command[i];
            //System.out.println(word);
            if (word.equals("where")){
                parseTree.whereID=i;
                parseTree.where=true;
            }
        }
        if(parseTree.where){
            parseTree.tables= new ArrayList<String>(Arrays.asList(Arrays.copyOfRange(Command, parseTree.fromID+1, parseTree.whereID)));
            String[] condition=Arrays.copyOfRange(Command, parseTree.whereID+1,Command.length);
            parseTree.expressionTree= new ExpressionTree(condition);
            parseTree.expressionTree.PrintTreeNode();
        }else{
            parseTree.tables= new ArrayList<String>(Arrays.asList(Arrays.copyOfRange(Command, parseTree.fromID+1, Command.length)));
        }
        stmt.parseTree = parseTree;
        return stmt;
    }

    public Statement insertStatement(String sql){
        Statement stmt = new Statement();
        String regex = "insert[\\s]+into[\\s]+(.+)[\\s]+\\((.*)\\)[\\s]+values[\\s]+(.*)";
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(sql);
        if(m.find()){
            stmt.tableName = m.group(1);
            stmt.fieldNames = new ArrayList<>(Arrays.asList(m.group(2).split("[\\s]*,[\\s]*")));
            String[] values = m.group(3).replace("(", "").replace(")", "").split("[\\s]*,[\\s]*");
            int len = stmt.fieldNames.size();
            ArrayList<String> tmp = new ArrayList<>();
            for(int i = 0; i < values.length; ++i){
                if(i % len == 0) {
                    if(i != 0) stmt.fieldValues.add(tmp);
                    tmp.clear();
                }
                tmp.add(values[i]);
            }
            stmt.fieldValues.add(tmp);
        }else{
            System.out.println("Invalid SQL");
        }
        //System.out.println(stmt.fieldValues.get(0).size());
        return stmt;
    }

    public static void main(String[] args) throws IOException{
        String test1 = "create table table_name (c1 int, c2 int, c3 int)";
        String test2 = "insert into table_name (c1, c2, c3) values (1, 2, 3)";
        String test3 = "select * from table_name where a > 3 and c = 2";
        Parser p = new Parser();
        p.createStatement(test1);
        p.insertStatement(test2);
        p.selectStatement(test3);
    }
}

