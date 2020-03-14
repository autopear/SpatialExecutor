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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class ReadWorker extends IOWoker {
    private BufferedWriter readLogWriter;

    public ReadWorker(Configuration config, AtomicLong pkid, long startTime) {
        super(config, pkid, config.getBatchSizeRead(), startTime, "Read:   ");
        if (startTime < 1)
            System.out.println("Read: size = " + maxOps + ", threads = " + config.getNumThreadsRead() + ", sleep = " + config.getSleepRead() + ", exp = [" + config.getMinExp() + ", " + config.getMaxExp() + "]");
        else
            System.out.println("Read: duration = " + config.getDuration() + ", threads = " + config.getNumThreadsRead() + ", sleep = " + config.getSleepRead() + ", exp = [" + config.getMinExp() + ", " + config.getMaxExp() + "]");
    }

    public Pair<Long, Long> execute() throws InterruptedException {
        reset();
        try {
            readLogWriter = new BufferedWriter(new FileWriter(config.getReadLogPath(), true));
        } catch (IOException e) {
            e.printStackTrace();
            readLogWriter = null;
        }
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
        if (readLogWriter != null) {
            try {
                readLogWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
                readLogWriter = null;
            }
        }
        return Pair.of(showProgress(true), System.currentTimeMillis() - localStartTime);
    }

    private synchronized void writeLog(List<String> lines) {
        if (readLogWriter != null && !lines.isEmpty()) {
            for (String line : lines) {
                try {
                    readLogWriter.write(line);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    protected class ReadThread extends IOThread {
        private final JSONParser parser;

        private ReadThread(int tid, long totoalOps) {
            super(tid, totoalOps);
            parser = new JSONParser();
        }

        private int randExp() {
            return ThreadLocalRandom.current().nextInt(config.getMinExp(), config.getMaxExp() + 1);
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

        public void task() {
            DBConnector connector = new DBConnector("http://" + config.getPrivateIP() + ":19002/query/service");
            List<String> results = new ArrayList<>();
            long total_records = pkid.get();
            String sqlErr = "";
            for (long performedOps = 0; getTotoalOps() < 1 || performedOps < getTotoalOps(); performedOps++) {
                int exp = randExp();
                double w = config.getSpaceWidth() * Math.pow(10, -exp);
                double h = config.getSpaceHeight() * Math.pow(10, -exp);
                double x = ThreadLocalRandom.current().nextDouble(0f, config.getSpaceWidth() - w);
                double y = ThreadLocalRandom.current().nextDouble(0f, config.getSpaceHeight() - h);
                Pair<Long, Long> res = parseResult(connector.execute(query(x, y, w, h), sqlErr));
                if (res.getLeft() >= 0 && res.getRight() > 0)
                    results.add(total_records + "\t" + exp + "\t" + x + "\t" + y + "\t" + res.getLeft() + "\t" + res.getRight() + "\n");
                else
                    Utils.print(sqlErr + "\n");
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
        }
    }
}
