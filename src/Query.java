import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import storageManager.*;

public class Query {
    public Parser parser;
    public MainMemory Memory;
    public Disk disk;
    public SchemaManager schemaMG;

    public Query(){
        parser = null;
        Memory = new MainMemory();
        disk = new Disk();
        schemaMG=new SchemaManager(Memory, disk);
        disk.resetDiskIOs();
        disk.resetDiskTimer();
    }

    public void ParseQuery(String m) throws Exception{
        String[] Command= m.trim().toLowerCase().split("\\s");
        String first = Command[0];
        parser = new Parser();
        parser.Parse(m);
        switch (first){
            case "create": this.CreateQuery(m);
                break;
            case "select": this.SelectQuery(m);
                break;
            case "drop"  : this.DropQuery(m);
                break;
            case "delete": this.DeleteQuery(m);
                break;
            case "insert": this.InsertQuery(m);
                break;
            default: throw new Exception("Not a legal command!");
        }
    }

    public void CreateQuery(String m){
        ArrayList<String> field_names= parser.fields;
        ArrayList<FieldType> field_types= parser.fieldtypes;
        Schema schema=new Schema(field_names,field_types);
        //schemaMG.createRelation(parser.table_name,schema);
        Relation relation_reference=schemaMG.createRelation(parser.table_name,schema);
        System.out.print(relation_reference.getSchema() + "\n");

    }

    public void SelectQuery(String m){

    }

    public void DropQuery(String m){

    }

    public void DeleteQuery(String m){

    }

    public void InsertQuery(String m){

    }

    public static void main(String[] args) throws IOException {
        BufferedReader br =new BufferedReader(new InputStreamReader(System.in));
        String s = br.readLine();
        Parser parse = new Parser();
        Query query= new Query();
        try {
            query.ParseQuery(s);
        }catch(Exception e){
            System.out.println("Got an Exception:"+e.getMessage());
            e.printStackTrace();
        }
        br.close();
    }
}
