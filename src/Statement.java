import java.util.ArrayList;

import storageManager.FieldType;

public class Statement {
    String tableName;
    ArrayList<String> fieldNames;
    ArrayList<FieldType> fieldTypes;
    ArrayList<ArrayList<String>> fieldValues;
    Statement(){
        fieldNames = new ArrayList<>();
        fieldTypes = new ArrayList<>();
        fieldValues = new ArrayList<>();
    }
}
