public class RoundMismatchException extends Exception {
    int curRound;

    public RoundMismatchException(String msg, int curRound) {
        super(msg);
        this.curRound = curRound;
    }

    public int getCurRound() {
        return curRound;
    }
}