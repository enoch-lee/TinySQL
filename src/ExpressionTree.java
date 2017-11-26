import storageManager.FieldType;
import storageManager.Tuple;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Stack;
import java.util.Queue;

public class ExpressionTree {
    private static Queue<String> postExpr;
    private static HashMap<Character, Integer> precedence;
    ExpressionTree(){
        postExpr = new LinkedList<>();
        precedence = new HashMap<>();
        precedence.put('(', 1);
        precedence.put(')', 1);
        precedence.put('+', 2);
        precedence.put('-', 2);
        precedence.put('*', 3);
        precedence.put('/', 3);
        precedence.put('>', 3);
        precedence.put('<', 3);
        precedence.put('=', 3);
        precedence.put('|', 3);
        precedence.put('&', 3);
        precedence.put('^', 3);
    }
    public static void infix2postfix(String expr){
        char[] tokens = expr.toCharArray();
        Stack<Character> ops = new Stack<Character>();
        for(int i = 0; i < tokens.length; ++i){
            if(tokens[i] == ' ') continue;
            if (tokens[i] >= '0' && tokens[i] <= '9') {
                StringBuffer str = new StringBuffer();
                while (i < tokens.length && tokens[i] >= '0' && tokens[i] <= '9')
                    str.append(tokens[i++]);
                postExpr.add(str.toString());
            }
            else if (tokens[i] == '('){
                ops.push(tokens[i]);
            }
            else if (tokens[i] == ')') {
                while (ops.peek() != '('){
                    postExpr.add(ops.peek().toString());
                    ops.pop();
                }
                ops.pop();
            }
            else if (tokens[i] == '+' || tokens[i] == '-' ||
                    tokens[i] == '*' || tokens[i] == '/') {
                while (!ops.empty() && (precedence.get(tokens[i]) >= precedence.get(ops.peek()))){
                    postExpr.add(ops.peek().toString());
                    ops.pop();
                };
                ops.push(tokens[i]);
            }
            else if(tokens[i] == 'a'){
                StringBuffer str = new StringBuffer();
                for(int j = 0; j < 3; ++j) str.append(tokens[i++]);

            }
            else if(tokens[i] == 'n'){

            }
            else if(tokens[i] == 'o'){

            }
            else {
                System.out.println("Invalid Token!");
            }
        }
        while(!ops.empty()){
            postExpr.add(ops.peek().toString());
            ops.pop();
        }
    }

    public void evaluate(){
        Stack<String> tmp = new Stack<>();
        while(!postExpr.isEmpty()){
            if(isOperator(postExpr.peek())){
                String operator = postExpr.peek();
                String op1 = tmp.peek();
                tmp.pop();
                String op2 = tmp.peek();
                tmp.pop();
            }
            else{
                tmp.push(postExpr.peek());
                postExpr.poll();
            }
        }
    }

    private int calculate(String op1, String op2, String operator){
        switch (operator) {
            case "+": {
                return Integer.parseInt(op1) + Integer.parseInt(op2);
            }
            case "-": {
                return Integer.parseInt(op1) - Integer.parseInt(op2);
            }
            case "*": {
                return Integer.parseInt(op1) * Integer.parseInt(op2);
            }
            case "/": {
                return Integer.parseInt(op1) / Integer.parseInt(op2);
            }
            default: try {
                throw new Exception("Unknown Operator");
            }catch (Exception err) {
                err.printStackTrace();
            }
        }
        return 0;
    }

    private void calculate1(String op1, String op2, String operator){
        switch (operator) {
            case "AND": {

            }
            case "OR": {

            }
            case "=": {

            }
            case ">": {

            }
            case "<": {

            }
            case "NOT": {

            }
            default: try {
                throw new Exception("Unknown Operator");
            }catch (Exception err) {
                err.printStackTrace();
            }
        }
    }

    private boolean isOperator(String op){
        if(op.equals("+") || op.equals("-") || op.equals("*") || op.equals("/") ||
                op.equals("|") || op.equals("&") || op.equals("^")){
            return true;
        }
        return false;
    }

    public static void main(String[] args){
        ExpressionTree exprTree = new ExpressionTree();
        String test = "1 + 2 * 3";
        exprTree.infix2postfix(test);
        exprTree.evaluate();
    }
}
