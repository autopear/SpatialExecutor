package edu.ucr.cs.SpatialLSM.common;

import java.io.FileWriter;
import java.io.IOException;

public class ThroughputLogger {
    private static ThroughputLogger logger;

    private final String logPath;
    private final int interval;
    private long numWrites;
    private long numReads;
    private long startTime;
    private long lastTime;

    public static void updateStats(long newWrites, long newReads) {
        if (logger != null)
            logger.update(newWrites, newReads);
    }

    public static void createLogger(String logPath, int interval) {
        if (logger == null)
            logger = new ThroughputLogger(logPath, interval);
    }

    public static String logFilePath() {
        return logger == null ? "" : logger.getLogFilePath();
    }

    public ThroughputLogger(String logPath, int interval) {
        this.logPath = logPath;
        this.interval = interval;
        numWrites = 0;
        numReads = 0;
        startTime = 0;
        lastTime = 0;
    }

    private String getLogFilePath() {
        return logPath;
    }

    private synchronized void update(long newWrites, long newReads) {
        boolean isInit = false;
        if (startTime == 0) {
            startTime = System.nanoTime();
            lastTime = startTime;
            isInit = true;
        }
        if (isInit || newWrites > 0 || newReads > 0) {
            if (newWrites > 0)
                numWrites += newWrites;
            if (newReads > 0)
                numReads += newReads;
            long currentTime = System.nanoTime();
            if (isInit || currentTime - lastTime >= interval * 1000000000) {
                if (!isInit)
                    lastTime = currentTime;
                try {
                    FileWriter fw = new FileWriter(logPath, true);
                    fw.write((lastTime - startTime) + "\t" + numWrites + "\t" + numReads + "\n");
                    fw.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

}
