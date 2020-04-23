package edu.ucr.cs.SpatialLSM.impls;

import edu.ucr.cs.SpatialLSM.common.Configuration;
import edu.ucr.cs.SpatialLSM.common.Utils;
import org.apache.commons.lang3.tuple.Pair;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import edu.ucr.cs.SpatialLSM.apis.IOWoker;
import edu.ucr.cs.SpatialLSM.apis.IOThread;
import edu.ucr.cs.SpatialLSM.common.DBConnector;

import java.io.IOException;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

public class ReadWorker extends IOWoker {
    private final String tmpReadLogPath;
    private final byte[] numBuf = new byte[Float.BYTES * 3];

    public ReadWorker(Configuration config, GZIPInputStream gis, AtomicLong pkid, long startTime) {
        super(config, gis, pkid, config.getBatchSizeRead(), startTime, "Read:   ");
        tmpReadLogPath = config.getReadLogPath() + ".tmp";

        if (startTime < 1)
            System.out.println("Read: size = " + maxOps + ", threads = " + config.getNumThreadsRead() + ", sleep = " + config.getSleepRead());
        else
            System.out.println("Read: duration = " + config.getDuration() + ", threads = " + config.getNumThreadsRead() + ", sleep = " + config.getSleepRead());
    }

    public int clearTmpFiles() {
        File dir = new File(config.getLogsDir());
        File [] tmpFiles = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                if (name.startsWith(config.getTaskName() + ".read.tsv.tmp.")) {
                    String last = name.replace(config.getTaskName() + ".read.tsv.tmp.", "");
                    try {
                        int id = Integer.parseInt(last);
                        return true;
                    } catch (NumberFormatException nfe) {
                        return false;
                    }
                }
                return false;
            }
        });
        for (File tmpFile : tmpFiles) {
            tmpFile.delete();
        }
        return tmpFiles.length;
    }

    public int sortMergeTmpFiles() {
        File dir = new File(config.getLogsDir());
        File [] tmpFiles = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                if (name.startsWith(config.getTaskName() + ".read.tsv.tmp.")) {
                    String last = name.replace(config.getTaskName() + ".read.tsv.tmp.", "");
                    try {
                        int id = Integer.parseInt(last);
                        return true;
                    } catch (NumberFormatException nfe) {
                        return false;
                    }
                }
                return false;
            }
        });
        if (tmpFiles.length > 0) {
            StringBuilder sb = new StringBuilder(tmpFiles[0].getAbsolutePath());
            for (int i = 1; i < tmpFiles.length; i++)
                sb.append(" ").append(tmpFiles[i].getAbsolutePath());
            Utils.runCommand("cat " + sb.toString() + " | sort -n -k1,1 -k2,2 > " + config.getReadLogPath());
            for (File tmpFile : tmpFiles) {
                tmpFile.delete();
            }
        }
        return tmpFiles.length;
    }

    public Pair<Long, Long> execute() throws InterruptedException {
        reset();
        long localStartTime = System.currentTimeMillis();
        if (config.getNumThreadsRead() == 1) {
            ReadThread w = new ReadThread(0, maxOps);
            w.task();
        } else {
            long batch = (long) Math.ceil((double) maxOps / config.getNumThreadsRead());

            List<ReadThread> threads = new ArrayList<>();
            for (int i = 0; i < config.getNumThreadsRead() - 1; i++)
                threads.add(new ReadThread(i + 1, batch));
            threads.add(new ReadThread(config.getNumThreadsRead(), maxOps - batch * (config.getNumThreadsRead() - 1)));

            for (ReadThread w : threads)
                w.start();
            for (ReadThread w : threads)
                w.join();
        }
        Pair<Long, Long> ret = Pair.of(showProgress(true), System.currentTimeMillis() - localStartTime);
        return ret;
    }

    protected class ReadThread extends IOThread {
        private final JSONParser parser;
        private BufferedWriter readLogWriter;

        private ReadThread(int tid, long totoalOps) {
            super(tid, totoalOps);
            parser = new JSONParser();
        }

        private String query(double x, double y, double w, double h) {
            return "USE Level_Spatial;" +
                    "SELECT COUNT(*) AS cnt FROM Spatial_Table WHERE SPATIAL_INTERSECT(geo, rectangle(\"" +
                    x + "," + y + " " + (x + w) + "," + (y + h) + "\"));";
        }

        private Pair<Long, Long> parseResult(String result) {
            if (!result.isEmpty()) {
                try {
                    JSONObject resObj = (JSONObject) parser.parse(result);
                    long cnt = Long.parseLong(((JSONObject) ((JSONArray) resObj.get("results")).get(0)).get("cnt").toString());
                    String timeStr = ((JSONObject) resObj.get("metrics")).get("executionTime").toString().trim().toLowerCase();
                    long time;
                    if (timeStr.endsWith("ns"))
                        time = Math.round(Double.parseDouble(timeStr.replace("ns", "")) / 1000000);
                    else if (timeStr.endsWith("us"))
                        time = Math.round(Double.parseDouble(timeStr.replace("us", "")) / 1000);
                    else if (timeStr.endsWith("ms"))
                        time = Math.round(Double.parseDouble(timeStr.replace("ms", "")));
                    else if (timeStr.endsWith("s"))
                        time = Math.round(Double.parseDouble(timeStr.replace("s", "")) * 1000);
                    else if (timeStr.endsWith("m"))
                        time = Math.round(Double.parseDouble(timeStr.replace("m", "")) * 60000);
                    else
                        time = Math.round(Double.parseDouble(timeStr.replace("h", "")) * 3600000);
                    return Pair.of(cnt, time);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
            return Pair.of(-1L, -1L);
        }

        private void writeLog(List<String> lines) {
            if (readLogWriter != null && !lines.isEmpty()) {
                for (String line : lines) {
                    try {
                        readLogWriter.write(line);
                        readLogWriter.flush();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        public void task() {
            try {
                readLogWriter = new BufferedWriter(new FileWriter(tmpReadLogPath + "." + getTid(), true));
            } catch (IOException e) {
                e.printStackTrace();
                readLogWriter = null;
            }
            DBConnector connector = new DBConnector("http://" + config.getPrivateIP() + ":19002/query/service");
            List<String> results = new ArrayList<>();
            String sqlErr = "";
            try {
                for (long performedOps = 0; getTotoalOps() < 1 || performedOps < getTotoalOps(); performedOps++) {
                    gzis.read(numBuf);
                    float exp = (float)(Utils.bytes2double(numBuf, 0, Float.BYTES));
                    double x = Utils.bytes2double(numBuf, Float.BYTES, Float.BYTES);
                    double y = Utils.bytes2double(numBuf, Float.BYTES * 2, Float.BYTES);

                    double w = 360.0 / Math.pow(10, exp);
                    double h = 180.0 / Math.pow(10, exp);
                    Pair<Long, Long> res = parseResult(connector.execute(query(x, y, w, h), sqlErr));
                    if (res.getLeft() >= 0 && res.getRight() > 0)
                        results.add(pkid.get() + "\t" + exp + "\t" + x + "\t" + y + "\t" + res.getLeft() + "\t" + res.getRight() + "\n");
                    else
                        Utils.print(sqlErr + "\n");
                    if (results.size() == 100) {
                        writeLog(results);
                        results.clear();
                    }
                    showProgress(false);
                    if (startTime > 0 && System.currentTimeMillis() - startTime >= config.getDuration())
                        break;
                    if (config.getSleepRead() > 0) {
                        long sleepTime;
                        if (startTime > 0 && config.getDuration() > 0)
                            sleepTime = Math.min(config.getSleepRead(), config.getDuration() * 1000 + startTime - System.currentTimeMillis());
                        else
                            sleepTime = config.getSleepRead();
                        try {
                            sleep(sleepTime);
                        } catch (InterruptedException e) {
                            break;
                        }
                        if (startTime > 0 && System.currentTimeMillis() - startTime >= config.getDuration())
                            break;
                    }
                }
                connector.close();
                writeLog(results);
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (readLogWriter != null) {
                try {
                    readLogWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    readLogWriter = null;
                }
            }
        }
    }
}
