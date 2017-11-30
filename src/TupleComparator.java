import storageManager.Field;
import storageManager.FieldType;
import storageManager.Tuple;

import java.util.Comparator;

public class TupleComparator implements Comparator<Tuple> {
    private String fieldName;
    private static int campareFields(Field a, Field b){
        if(!a.type.equals(b.type)){
            System.err.println("Fields Type cannot match!");
        }
        if(a.type.equals(FieldType.INT)){
            return a.integer - b.integer;
        }else{
            return a.str.compareTo(b.str);
        }
    }
    TupleComparator(){}
    TupleComparator(String fieldName){ this.fieldName = fieldName; }
    @Override
    public int compare(Tuple t1, Tuple t2){
        if(fieldName == null){
            for(int i = 0; i < t1.getNumOfFields(); ++i){
                if(campareFields(t1.getField(i), t2.getField(i)) != 0){
                    //default in ascending order
                    return campareFields(t1.getField(i), t2.getField(i));
                }
            }
        }else{
            return campareFields(t1.getField(fieldName), t2.getField(fieldName));
        }

        return 0;
    }
};