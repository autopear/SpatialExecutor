package edu.ucr.cs.SpatialLSM.apis;

import edu.ucr.cs.SpatialLSM.common.Configuration;
import edu.ucr.cs.SpatialLSM.common.Utils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

public abstract class IOWoker extends Thread {
    protected final Configuration config;
    protected final GZIPInputStream gzis;
    protected final AtomicLong pkid;
    protected final long maxOps;
    protected final long startTime;
    private final AtomicLong ops;
    private long percent;
    private final String logPrefix;
    private Pair<Long, Long> result;

    protected IOWoker(Configuration config, GZIPInputStream gis, AtomicLong pkid, long maxOps, long startTime, String logPrefix) {
        this.config = config;
        this.gzis = gis;
        this.pkid = pkid;
        this.maxOps = maxOps;
        this.startTime = startTime;
        ops = new AtomicLong(0);
        this.logPrefix = logPrefix;
        result = Pair.of(-1L, -1L);
    }

    public Pair<Long, Long> getResult() {
        return result;
    }

    @Override
    public void run() {
        try {
            result = execute();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public abstract Pair<Long, Long> execute() throws InterruptedException;

    protected void reset() {
        percent = 0;
        ops.set(0);
    }

    protected synchronized long showProgress(boolean isFinal) {
        if (isFinal) {
            long numOps = ops.get();
            if (maxOps < 1)
                Utils.print(logPrefix + Utils.num2str(numOps) + ", elapsed " + Utils.durationToString(Math.round((double) (System.currentTimeMillis() - startTime) / 1000)) + "\n");
            else if (startTime > 0)
                Utils.print(logPrefix + Utils.num2str(numOps) + " / " + Utils.num2str(maxOps) + " (" + ((double)percent / 10) + "%), elapsed " + Utils.durationToString(Math.round((double) (System.currentTimeMillis() - startTime) / 1000)) + "\n");
            else
                Utils.print(logPrefix + Utils.num2str(numOps) + " / " + Utils.num2str(maxOps) + " (" + ((double)percent / 10) + "%)\n");
            return numOps;
        } else {
            long numOps = ops.incrementAndGet();
            if (maxOps < 1) {
                long np = Math.round((double) (System.currentTimeMillis() - startTime) / 1000);
                if (np - percent >= 10) {
                    percent = np;
                    Utils.print(logPrefix + Utils.num2str(numOps) + ", elapsed " +  Utils.durationToString(percent) + "\r");
                }
            } else {
                int np = (int)Math.round((double) numOps * 1000 / maxOps);
                if (np > percent) {
                    percent = np;
                    if (startTime > 0)
                        Utils.print(logPrefix + Utils.num2str(numOps) + " / " + Utils.num2str(maxOps) + " (" + ((double)percent / 10) + "%), elapsed " + Utils.durationToString(Math.round((double) (System.currentTimeMillis() - startTime) / 1000)) + "\r");
                    else
                        Utils.print(logPrefix + Utils.num2str(numOps) + " / " + Utils.num2str(maxOps) + " (" + ((double)percent / 10) + "%)\r");
                }
            }
            return numOps;
        }
    }
}
