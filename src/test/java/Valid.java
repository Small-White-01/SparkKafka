import java.util.Stack;

public class Valid {

    public static void main(String[] args) {
        String s="]";
        System.out.println(isCirCle(s));
    }

    public static boolean isCirCle(String s){

        //通过栈，左括号加入栈，遇到右括号,判断栈顶

        char[] chars = s.toCharArray();
        Stack<Character> stack=new Stack<>();
        for (char c:chars){

            switch (c){

                case '(':
                case '{':
                case '[':{
                    stack.push(c);
                    break;
                }
                case ')':{
                    if(stack.peek().equals('(')){
                        stack.pop();
                        break;
                    }
                    return false;

                }
                case '}':{
                    if(stack.peek().equals('{')){
                        stack.pop();
                        break;
                    }
                    return false;
                }
                case ']':{
                    if(!stack.isEmpty()&&stack.peek().equals('[')){
                        stack.pop();
                        break;
                    }
                    return false;
                }
            }


        }
        if(!stack.isEmpty())return false;
        return true;


    }
}
