import storageManager.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Stack;

public class ExpressionTree {
    public boolean choose;

    public boolean AndOR;
    public ArrayList<Integer> AndORs;
    public ArrayList<String> AndOrNots;
    public ArrayList<String[]> subconditions;
    public ArrayList<TreeNode> Roots;

    private Stack<String> OperatorStack;
    private Stack<TreeNode> TreeNodeStack;


    ExpressionTree(String[] condition){
        this.OperatorStack=new Stack<String>();
        this.TreeNodeStack=new Stack<TreeNode>();

        this.Roots=new ArrayList<TreeNode>();
        this.AndORs=new ArrayList<Integer>();
        this.AndOrNots=new ArrayList<String>();
        this.subconditions=new ArrayList<String[]>();

        SplitSubCondition(condition);
        if(!AndOR){
            Roots.add(Build(condition));
        }else{
            for(int i=0; i<subconditions.size();i++){
                String[] condi=subconditions.get(i);
                Roots.add(Build(condi));
            }
        }
    }

    public TreeNode Build(String[] condition){
        TreeNode root=new TreeNode();
        if(condition[0].equals("not")){
            condition=Arrays.copyOfRange(condition,1,condition.length);
        }
        for(String con:condition){
            int kind = Judge(con);
            if(kind==1){
                Operator(con);
            }else if(kind==2){
                LeftParenthesis();
            }else if(kind==3){
                RightParenthesis();
            }else{
                Words(con);
            }
        }
        while(!OperatorStack.empty()){
            NodeOperation(OperatorStack.pop());
        }
        root=TreeNodeStack.pop();
        return root;
    }

    public void SplitSubCondition(String[] condition){
        AndOR=false;

        for(int i=0; i<condition.length; i++){
            String word= condition[i];
            if(word.equals("and")||word.equals("or")){
                AndOR=true;
                if(word.equals("and")){
                    AndOrNots.add("and");
                }else{
                    AndOrNots.add("or");
                }
                AndORs.add(i);
            }else{
                if(word.equals("not")){
                    AndOrNots.add("not");
                }
            }
        }
        if(AndOR){
            if(AndORs.size()==1){
                String[] subcondition=Arrays.copyOfRange(condition,0, AndORs.get(0));
                subconditions.add(subcondition);
                subcondition=Arrays.copyOfRange(condition, AndORs.get(0)+1,condition.length);
                subconditions.add(subcondition);
            }else {
                String[] subcondition = Arrays.copyOfRange(condition, 0, AndORs.get(0));
                subconditions.add(subcondition);
                //System.out.println("1");
                for (int i = 0; i < AndORs.size() - 1; i++) {
                    subcondition = Arrays.copyOfRange(condition, AndORs.get(i) + 1, AndORs.get(i + 1));
                    subconditions.add(subcondition);
                    //System.out.println("2");
                }
                subcondition = Arrays.copyOfRange(condition, AndORs.get(AndORs.size() - 1) + 1, condition.length);
                subconditions.add(subcondition);
                //System.out.println("3");
            }
        }
    }

    public void Operator(String word){
        int order = getOrder(word);
        if(!OperatorStack.empty() && order<=getOrder(OperatorStack.peek())){
            NodeOperation(OperatorStack.pop());
        }
        OperatorStack.push(word);
    }

    public int Judge(String word){
        if(word.equals("+")||word.equals("-")||word.equals("*")||word.equals("/")||word.equals("=")||word.equals(">")||word.equals("<")){
            return 1;
        }else if(word.equals("(")){
            return 2;
        }else if(word.equals(")")){
            return 3;
        }else{
            return 4;
        }

    }//1 is operators, 2 is left parenthesis, 3 is right parenthesis, 4 is operand

    public int getOrder(String word){
        if(word.equals("/")||word.equals("*")) return 2;
        else if(word.equals("+")||word.equals("-")||word.equals(">")||word.equals("<")) return 1;
        else if(word.equals("=")) return 0;
        else return -1;
    }

    public void NodeOperation(String rootName){
        TreeNode right=TreeNodeStack.pop();//first right, then left
        TreeNode left=TreeNodeStack.pop();
        TreeNode root =new TreeNode(rootName,left,right);
        TreeNodeStack.push(root);
    }

    public void LeftParenthesis(){
        OperatorStack.push("(");
    }

    public void RightParenthesis(){
        while(!OperatorStack.empty() && !OperatorStack.peek().equals("(")){
            NodeOperation(OperatorStack.pop());
        }
        OperatorStack.pop(); // pop out "("
    }

    public void Words(String rootName){
        TreeNode root= new TreeNode(rootName);
        TreeNodeStack.push(root);
    }

    public void PrintTreeNode() {
        for(int i=0; i<Roots.size(); i++) {
            TreeNode root=Roots.get(i);
            PrintTraversal(root);
        }
    }

    public boolean checkTuple(Tuple tuple){
        ArrayList<String> TotalResult=new ArrayList<String>();
        for(int i=0; i<Roots.size(); i++){
            TotalResult.add(ReturnResults(tuple, Roots.get(i)));
        }
        int j=0;
        for(int i=0; i<AndOrNots.size(); i++){
            String condition=AndOrNots.get(i);
            if(condition.equals("not")){
                if(TotalResult.get(j).equals("true"))
                    TotalResult.set(j,"false");
                else TotalResult.set(j,"true");
            }else{
                if(condition.equals("and")){
                    if(TotalResult.get(j).equals("true")&&TotalResult.get(j+1).equals("true")){

                    }else{
                        TotalResult.set(j,"false");
                        TotalResult.set(j+1,"false");
                    }
                }else{
                    if(TotalResult.get(j).equals("true")||TotalResult.get(j+1).equals("true")){
                        TotalResult.set(j,"true");
                        TotalResult.set(j+1,"true");
                    }
                }
                j++;
            }
        }
        return TotalResult.get(TotalResult.size() - 1).equals("true");
    }

    private void PrintTraversal(TreeNode root){
        if(root.leftchild!=null) PrintTraversal(root.leftchild);
        //root.PrintNode();
        if(root.rightchild!=null) PrintTraversal(root.rightchild);
    }

    private String ReturnResults(Tuple tuple, TreeNode root){
        String word=root.root;
        if(word.equals("+")){
            String left=ReturnResults(tuple, root.leftchild);
            String right=ReturnResults(tuple, root.rightchild);
            int Intleft=Integer.parseInt(left);
            int Intright=Integer.parseInt(right);
            return String.valueOf(Intleft+Intright);
        }else if(word.equals("*")){
            String left=ReturnResults(tuple, root.leftchild);
            String right=ReturnResults(tuple, root.rightchild);
            int Intleft=Integer.parseInt(left);
            int Intright=Integer.parseInt(right);
            return String.valueOf(Intleft*Intright);
        }else if(word.equals("-")){
            String left=ReturnResults(tuple, root.leftchild);
            String right=ReturnResults(tuple, root.rightchild);
            int Intleft=Integer.parseInt(left);
            int Intright=Integer.parseInt(right);
            return String.valueOf(Intleft-Intright);
        }else if(word.equals("/")){
            String left=ReturnResults(tuple, root.leftchild);
            String right=ReturnResults(tuple, root.rightchild);
            int Intleft=Integer.parseInt(left);
            int Intright=Integer.parseInt(right);
            return String.valueOf(Intleft/Intright);
        }else if(word.equals("<")){
            String left=ReturnResults(tuple, root.leftchild);
            String right=ReturnResults(tuple, root.rightchild);
            int Intleft=Integer.parseInt(left);
            int Intright=Integer.parseInt(right);
            return String.valueOf(Intleft<Intright);
        }else if(word.equals(">")){
            String left=ReturnResults(tuple, root.leftchild);
            String right=ReturnResults(tuple, root.rightchild);
            int Intleft=Integer.parseInt(left);
            int Intright=Integer.parseInt(right);
            return String.valueOf(Intleft>Intright);
        }else if(word.equals("=")){
            String left=ReturnResults(tuple, root.leftchild);
            String right=ReturnResults(tuple, root.rightchild);
            if(isInteger(left)&&isInteger(right)){
                int Intleft=Integer.parseInt(left);
                int Intright=Integer.parseInt(right);
                return String.valueOf(Intleft==Intright);
            }else{
                return String.valueOf(left.equals(right));
            }
        }else if(isInteger(word)){
            return word;
        }else{
            if(!tuple.getField(word).toString().equals("")){
                return tuple.getField(word).toString();
            }else{
                return word;
            }
        }

    }

    public static boolean isInteger(String s){
        try{
            Integer.parseInt(s);
        }catch (NumberFormatException e){
            return false;
        }catch (NullPointerException e){
            return false;
        }
        return true;
    }
}
