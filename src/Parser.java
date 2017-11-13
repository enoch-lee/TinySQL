import javafx.util.Pair;
import storageManager.FieldType;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Parser {
    public String statement;
    public ArrayList<String> fields;
    public ArrayList<FieldType> fieldtypes;

    public Parser(){
        fields = new ArrayList<String>();
        fieldtypes = new ArrayList<FieldType>();

    }

    public String[] Parse(String m) throws Exception{
        String[] Command= m.trim().toLowerCase().split("\\s");
        String first = Command[0];
        switch (first){
            case "create": CreateStatement(m);
            break;
            case "select": SelectStatement(m);
            break;
            case "drop"  : DropStatement(m);
            break;
            case "delete": DeleteStatement(m);
            break;
            case "insert": InsertStatement(m);
            break;
            default: throw new Exception("Not a legal command!");
        }
        return Command;
    }

    public boolean CreateStatement(String m) throws Exception{
        if(checkCreate(m)) {
            Pattern pattern = Pattern.compile("\\((.+)\\)");
            Matcher matcher = pattern.matcher(m);
            matcher.find();
            String[] values = matcher.group(1).trim().split("[\\s]*,[\\s]*");
            for(String v:values) {
                String[] field=v.toLowerCase().split("\\s");
                String fieldT=field[1];
                FieldType type;
                if(fieldT.equals("STR20")){
                    type=FieldType.STR20;
                }else if(fieldT.equals("INT")){
                    type=FieldType.INT;
                }else{
                    throw new Exception("Wrong file type!!");
                }
                fields.add(field[0]);
                fieldtypes.add(type);
            }
            return true;
        }else{
            throw new Exception("No legal values");
        }
    }

    public boolean SelectStatement(String m){
        System.out.println("I'm in Select");
        String[] Command= m.trim().toLowerCase().split("\\s");
        return true;
    }

    public boolean DropStatement(String m){
        System.out.println("I'm in Drop");
        String[] Command= m.trim().toLowerCase().split("\\s");
        return true;
    }

    public boolean DeleteStatement(String m){
        System.out.println("I'm in Delete");
        String[] Command= m.trim().toLowerCase().split("\\s");
        return true;
    }

    public boolean InsertStatement(String m){
        System.out.println("I'm in Insert");
        String[] Command= m.trim().toLowerCase().split("\\s");
        return true;
    }


    public boolean checkCreate(String m){
        String[] Command = m.trim().toLowerCase().split("\\s");
        System.out.println("I'm in Create");
        if (Command.length <= 3) {
            System.out.println("Illegal Create");
            return false;
        }
        if (!Command[1].equalsIgnoreCase("table")) {
            System.out.println("Table should follow Create statement");
            return false;
        }

        if (!Command[3].startsWith("(") || !Command[Command.length - 1].endsWith(")")) {
            System.out.println("Miss parenthesizes");
            return false;
        }
        return true;
    }

    public static void main(String[] args) throws IOException{
        BufferedReader br =new BufferedReader(new InputStreamReader(System.in));
        String s = br.readLine();
        Parser P=new Parser();
        try {
            P.Parse(s);
        }catch(Exception e){
            System.out.println("Got an Exception:"+e.getMessage());
            e.printStackTrace();
        }
        br.close();
    }

}

