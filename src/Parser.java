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
        /*
        for(FieldType s : stmt.fieldTypes){
            System.out.println(s);
        }
        */
        return stmt;
    }

    private boolean SelectStatement(String m){
        System.out.println("I'm in Select");
        String[] Command= m.trim().toLowerCase().split("\\s");
        return true;
    }

    public String dropStatement(String sql){
        return sql.split("[\\s]+")[2];
    }

    private String deleteStatement(String sql){

        return null;
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
        Parser p = new Parser();
        p.createStatement(test1);
        p.insertStatement(test2);
    }
}

