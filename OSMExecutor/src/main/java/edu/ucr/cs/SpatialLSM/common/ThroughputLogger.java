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
    private final FileWriter fw;

    public static synchronized void updateStats(long newWrites, long newReads) {
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

    public static void flush() {
        if (logger != null) {
            try {
                logger.flushFile();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    public static void close() {
        if (logger != null) {
            try {
                logger.closeWriter();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    public ThroughputLogger(String logPath, int interval) throws IOException {
        this.logPath = logPath;
        this.interval = interval;
        numWrites = 0;
        numReads = 0;
        startTime = 0;
        fw = new FileWriter(logPath, false);
    }

    private String getLogFilePath() {
        return logPath;
    }

    private void closeWriter() throws IOException {
        fw.write((System.nanoTime() - startTime) + "\t" + numWrites + "\t" + numReads + "\n");
        fw.close();
    }

    private void update(long newWrites, long newReads) {
        if (startTime == 0) {
            startTime = System.nanoTime();
        }
        if (newWrites > 0)
            numWrites += newWrites;
        if (newReads > 0)
            numReads += newReads;
    }

    private void flushFile() throws IOException {
        fw.write((System.nanoTime() - startTime) + "\t" + numWrites + "\t" + numReads + "\n");
        fw.flush();
    }
}
