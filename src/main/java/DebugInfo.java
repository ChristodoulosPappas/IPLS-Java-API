public class DebugInfo {
    static boolean isEnabled = true;
    public DebugInfo(boolean isEnabled){
        this.isEnabled = isEnabled;
    }

    public static void log(Object o){
        if(isEnabled){
            System.out.println(o.toString());
        }
    }
}