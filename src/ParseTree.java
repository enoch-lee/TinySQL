import java.util.ArrayList;

public class ParseTree {
    public String type;
    public String dist_attribute;
    public String orderBy;

    public boolean distinct;
    public boolean where;
    public boolean order;

    public int fromID;
    public int whereID;
    public int orderID;
    public int distID;


    public ArrayList<String> attributes;
    public ArrayList<String> tables;

    public ExpressionTree expressionTree;


    ParseTree(String s){
        this.type = s;
        this.distinct = false;
        this.where = false;
        this.attributes = new ArrayList<String>();
        this.tables = new ArrayList<String>();
    }
}