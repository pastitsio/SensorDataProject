public class SensorMessageState {
    private SensorMessage prevMsg;
    private long timerSetFor;

    public SensorMessage getPrevMsg() {
        return prevMsg;
    }

    public void setPrevMsg(SensorMessage prevMsg) {
        this.prevMsg = prevMsg;
    }

    public long getTimerSetFor() {
        return timerSetFor;
    }

    public void setTimerSetFor(long timerSetFor) {
        this.timerSetFor = timerSetFor;
    }
}