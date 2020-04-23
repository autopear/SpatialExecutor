package edu.ucr.cs.SpatialLSM.apis;

public abstract class IOThread extends Thread {
    private final int tid;
    private final long totoalOps;

    protected IOThread(int tid, long totoalOps) {
        this.tid = tid;
        this.totoalOps = totoalOps;
    }

    public int getTid() {
        return tid;
    }

    public long getTotoalOps() {
        return totoalOps;
    }

    public abstract void task();

    @Override
    public void run() {
        task();
    }
}
