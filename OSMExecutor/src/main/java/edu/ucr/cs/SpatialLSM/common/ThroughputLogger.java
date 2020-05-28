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
    private final FileWriter fw;

    public static synchronized void updateStats(long newWrites, long newReads) throws IOException {
        if (logger != null)
            logger.update(newWrites, newReads);
    }

    public static void createLogger(String logPath, int interval) {
        if (logger == null) {
            try {
                logger = new ThroughputLogger(logPath, interval);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    public static String logFilePath() {
        return logger == null ? "" : logger.getLogFilePath();
    }

    public static void close() throws IOException {
        if (logger != null)
            logger.closeWriter();
    }

    public ThroughputLogger(String logPath, int interval) throws IOException {
        this.logPath = logPath;
        this.interval = interval;
        numWrites = 0;
        numReads = 0;
        startTime = 0;
        lastTime = 0;
        fw = new FileWriter(logPath, false);
    }

    private String getLogFilePath() {
        return logPath;
    }

    private void closeWriter() throws IOException {
        fw.write((System.nanoTime() - startTime) + "\t" + numWrites + "\t" + numReads + "\n");
        fw.close();
    }

    private void update(long newWrites, long newReads) throws IOException {
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
                fw.write((lastTime - startTime) + "\t" + numWrites + "\t" + numReads + "\n");
            }
        }
    }
}
