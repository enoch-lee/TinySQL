public class TreeNode {
    String root;
    TreeNode leftchild;
    TreeNode rightchild;

    TreeNode(){
        this.root=null;
        this.leftchild=null;
        this.rightchild=null;
    }
    TreeNode(String root){
        this.root=root;
        this.leftchild=null;
        this.rightchild=null;
    }
    TreeNode(String root, TreeNode left, TreeNode right){
        this.root=root;
        this.leftchild=left;
        this.rightchild=right;
    }

    void PrintNode(){
        System.out.println("the value is: "+this.root);
    }
}
