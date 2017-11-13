import java.util.ArrayList;
import storageManager.*;

public class Query {
    Parser parser;
    MainMemory Memory;
    Disk disk;
    SchemaManager schemaMG;

    public Query(){
        Parser parser = new Parser();
        MainMemory Memory = new MainMemory();
        Disk disk = new Disk();
        SchemaManager schemaMG=new SchemaManager(Memory, disk);
        disk.resetDiskIOs();
        disk.resetDiskTimer();
    }

    public void ParseQuery(String m) throws Exception{
        String[] Command= m.trim().toLowerCase().split("\\s");
        String first = Command[0];
        switch (first){
            case "create": CreateQuery(m);
                break;
            case "select": SelectQuery(m);
                break;
            case "drop"  : DropQuery(m);
                break;
            case "delete": DeleteQuery(m);
                break;
            case "insert": InsertQuery(m);
                break;
            default: throw new Exception("Not a legal command!");
        }
    }

    public void CreateQuery(String m){

    }

    public void SelectQuery(String m){

    }

    public void DropQuery(String m){

    }

    public void DeleteQuery(String m){

    }

    public void InsertQuery(String m){

    }
}
