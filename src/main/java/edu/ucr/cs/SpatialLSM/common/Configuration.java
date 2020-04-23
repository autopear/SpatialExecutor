package edu.ucr.cs.SpatialLSM.common;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.codec.binary.Hex;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Configuration {
    boolean configIsValid;
    private String nodeName;
    private String privateIP;
    private int feedPort;
    private String asterixDBPath;
    private String startScript;
    private String stopScript;
    private String resetDBPath;
    private String logParserPath;
    private String logsDir;
    private String readLogPath;
    private String taskName;
    private String rtreePolicy;
    private long duration = -1;
    private long sizeLoad = -1;
    private long batchSizeInsert = -1;
    private long batchSizeRead = -1;
    private int numBatchRead = -1;
    private int numBatchInsert = -1;
    private int numThreadsLoad = -1;
    private int numThreadsInsert = -1;
    private int numThreadsRead = -1;
    private double spaceWidth = -1;
    private double spaceHeight = -1;
    private double [] scales;
    private long loadSleep = -1;
    private long insertSleep = -1;
    private long readSleep = -1;

    public Configuration(final String configPath) {
        JSONParser parser = new JSONParser();
        try {
            Reader configReader = new FileReader(configPath);
            JSONObject jsonObject = (JSONObject) parser.parse(configReader);
            nodeName = jsonObject.get("node").toString();
            if (nodeName.compareTo("127.0.0.1") == 0 || nodeName.compareToIgnoreCase("localhost") == 0) {
                privateIP = "127.0.0.1";
            } else {
                privateIP = resolvePrivateIP(nodeName);
            }
            taskName = jsonObject.get("task").toString().trim();
            rtreePolicy = jsonObject.get("rtree").toString().replace("[\r\n]", " ").replace("\"", "\\\"");
            while (rtreePolicy.contains("  "))
                rtreePolicy = rtreePolicy.replace("  ", " ");
            if (jsonObject.containsKey("common")) {
                String commonConfigPath = jsonObject.get("common").toString();
                Reader commonReader = new FileReader(commonConfigPath);
                JSONObject commonObject = (JSONObject) parser.parse(commonReader);
                if (commonObject.containsKey("feed_port"))
                    feedPort = Integer.parseInt(commonObject.get("feed_port").toString());
                if (commonObject.containsKey("asterixdb"))
                    asterixDBPath = Utils.formatPath(commonObject.get("asterixdb").toString());
                if (commonObject.containsKey("reset_db"))
                    resetDBPath = Utils.formatPath(commonObject.get("reset_db").toString());
                if (commonObject.containsKey("log_parser"))
                    logParserPath = Utils.formatPath(commonObject.get("log_parser").toString());
                if (commonObject.containsKey("logs_dir"))
                    logsDir = Utils.formatPath(commonObject.get("logs_dir").toString());
                if (commonObject.containsKey("duration"))
                    duration = Long.parseLong(commonObject.get("duration").toString()) * 1000;
                if (commonObject.containsKey("size_load"))
                    sizeLoad = Long.parseLong(commonObject.get("size_load").toString());
                if (commonObject.containsKey("batch_insert"))
                    batchSizeInsert = Long.parseLong(commonObject.get("batch_insert").toString());
                if (commonObject.containsKey("batch_read"))
                    batchSizeRead = Long.parseLong(commonObject.get("batch_read").toString());
                if (commonObject.containsKey("num_insert"))
                    numBatchInsert = Integer.parseInt(commonObject.get("num_insert").toString());
                if (commonObject.containsKey("num_read"))
                    numBatchRead = Integer.parseInt(commonObject.get("num_read").toString());
                if (commonObject.containsKey("threads_load"))
                    numThreadsLoad = Integer.parseInt(commonObject.get("threads_load").toString());
                if (commonObject.containsKey("threads_insert"))
                    numThreadsInsert = Integer.parseInt(commonObject.get("threads_insert").toString());
                if (commonObject.containsKey("threads_read"))
                    numThreadsRead = Integer.parseInt(commonObject.get("threads_read").toString());
                if (commonObject.containsKey("space")) {
                    JSONArray space = (JSONArray) commonObject.get("space");
                    spaceWidth = Double.parseDouble(space.get(0).toString());
                    spaceHeight = Double.parseDouble(space.get(1).toString());
                }
                if (commonObject.containsKey("scales")) {
                    JSONArray arr = (JSONArray) commonObject.get("scales");
                    scales = new double[arr.size()];
                    for (int i = 0; i < arr.size(); i++)
                        scales[i] = str2d(arr.get(i).toString());
                }
                if (commonObject.containsKey("sleep_load"))
                    loadSleep = Long.parseLong(commonObject.get("sleep_load").toString());
                if (commonObject.containsKey("sleep_insert"))
                    insertSleep = Long.parseLong(commonObject.get("sleep_insert").toString());
                if (commonObject.containsKey("sleep_read"))
                    readSleep = Long.parseLong(commonObject.get("sleep_read").toString());
                commonReader.close();
            }
            if (jsonObject.containsKey("feed_port"))
                feedPort = Integer.parseInt(jsonObject.get("feed_port").toString());
            if (jsonObject.containsKey("asterixdb"))
                asterixDBPath = Utils.formatPath(jsonObject.get("asterixdb").toString());
            startScript = Utils.formatPath(asterixDBPath + "/opt/local/bin/start-sample-cluster.sh");
            stopScript = Utils.formatPath(asterixDBPath + "/opt/local/bin/stop-sample-cluster.sh");
            if (jsonObject.containsKey("reset_db"))
                resetDBPath = Utils.formatPath(jsonObject.get("reset_db").toString());
            if (jsonObject.containsKey("log_parser"))
                logParserPath = Utils.formatPath(jsonObject.get("log_parser").toString());
            if (jsonObject.containsKey("logs_dir"))
                logsDir = Utils.formatPath(jsonObject.get("logs_dir").toString());
            readLogPath = Utils.formatPath(logsDir + "/" + taskName + ".read.tsv");
            if (jsonObject.containsKey("duration"))
                duration = Long.parseLong(jsonObject.get("duration").toString()) * 1000;
            if (jsonObject.containsKey("size_load"))
                sizeLoad = Long.parseLong(jsonObject.get("size_load").toString());
            if (jsonObject.containsKey("batch_insert"))
                batchSizeInsert = Long.parseLong(jsonObject.get("batch_insert").toString());
            if (jsonObject.containsKey("batch_read"))
                batchSizeRead = Long.parseLong(jsonObject.get("batch_read").toString());
            if (jsonObject.containsKey("num_insert"))
                numBatchInsert = Integer.parseInt(jsonObject.get("num_insert").toString());
            if (jsonObject.containsKey("num_read"))
                numBatchRead = Integer.parseInt(jsonObject.get("num_read").toString());
            if (jsonObject.containsKey("threads_load"))
                numThreadsLoad = Integer.parseInt(jsonObject.get("threads_load").toString());
            if (jsonObject.containsKey("threads_insert"))
                numThreadsInsert = Integer.parseInt(jsonObject.get("threads_insert").toString());
            if (jsonObject.containsKey("threads_read"))
                numThreadsRead = Integer.parseInt(jsonObject.get("threads_read").toString());
            if (jsonObject.containsKey("space")) {
                JSONArray space = (JSONArray) jsonObject.get("space");
                spaceWidth = Double.parseDouble(space.get(0).toString());
                spaceHeight = Double.parseDouble(space.get(1).toString());
            }
            if (jsonObject.containsKey("scales")) {
                JSONArray arr = (JSONArray) jsonObject.get("scales");
                scales = new double[arr.size()];
                for (int i = 0; i < arr.size(); i++)
                    scales[i] = str2d(arr.get(i).toString());
            }
            if (jsonObject.containsKey("sleep_load"))
                loadSleep = Long.parseLong(jsonObject.get("sleep_load").toString());
            if (jsonObject.containsKey("sleep_insert"))
                insertSleep = Long.parseLong(jsonObject.get("sleep_insert").toString());
            if (jsonObject.containsKey("sleep_read"))
                readSleep = Long.parseLong(jsonObject.get("sleep_read").toString());
            configReader.close();
            configIsValid = true;
        } catch (IOException | ParseException e) {
            e.printStackTrace();
            configIsValid = false;
        }
    }

    private String resolvePrivateIP(final String host) {
        String output = Utils.getCommandOutput("ssh " + host + " \"hostname -I | awk '{{print $1}}'\"");
        return output.replaceAll("[\r\n]]", "").trim();
    }

    private static double str2d(String s) {
        String ss = s.toLowerCase();
        if (ss.contains("e"))
            return Double.parseDouble(ss);
        else {
            String[] ds = ss.split("e");
            if (ds.length != 2)
                throw new NumberFormatException(s + " cannot be parsed to double type.");
            double b = Double.parseDouble(ds[0]);
            double e = Double.parseDouble(ds[1].startsWith("+") ? ds[1].substring(1) : ds[1]);
            return Math.pow(b, e);
        }
    }

    public boolean isValid() {
        return configIsValid;
    }

    public String getNodeName() {
        return nodeName;
    }

    public String getPrivateIP() {
        return privateIP;
    }

    public boolean isLocalhost() {
        return privateIP.compareTo("127.0.0.1") == 0;
    }

    public int getFeedPort() {
        return feedPort;
    }

    public String getTaskName() {
        return taskName;
    }

    public String getReadLogPath() {
        return readLogPath;
    }

    public long getBatchSizeInsert() {
        return batchSizeInsert;
    }

    public double getSpaceHeight() {
        return spaceHeight;
    }

    public double getSpaceWidth() {
        return spaceWidth;
    }

    public int getNumThreadsInsert() {
        return numThreadsInsert;
    }

    public int getNumThreadsLoad() {
        return numThreadsLoad;
    }

    public long getDuration() {
        return duration;
    }

    public int getNumThreadsRead() {
        return numThreadsRead;
    }

    public long getBatchSizeRead() {
        return batchSizeRead;
    }

    public String getAsterixDBPath() {
        return asterixDBPath;
    }

    public long getSizeLoad() {
        return sizeLoad;
    }

    public String getResetDBPath() {
        return resetDBPath;
    }

    public String getLogParserPath() {
        return logParserPath;
    }

    public String getRtreePolicy() {
        return rtreePolicy;
    }

    public String startAsterixDBPath() {
        return startScript;
    }

    public String stopAsterixDBPath() {
        return stopScript;
    }

    public String getLogsDir() {
        return logsDir;
    }

    public double[] getScales() {
        return scales;
    }

    public long getSleepLoad() {
        return loadSleep;
    }

    public long getSleepInsert() {
        return insertSleep;
    }

    public long getSleepRead() {
        return readSleep;
    }

    public int getNumBatchInsert() {
        return numBatchInsert;
    }

    public int getNumBatchRead() {
        return numBatchRead;
    }

    public static double randDouble(double max) {
        return ThreadLocalRandom.current().nextDouble(0f, max);
    }

    public String newRecord(AtomicLong pkid) {
        return "{\"id\":" + pkid.incrementAndGet() + "," +
                "\"geo\":point(\"" + randDouble(spaceWidth) + "," + randDouble(spaceHeight) + "\")," +
                "\"data\":hex(\"" + randomString(1000) + "\")}";
    }

    public static String randomString(int l) {
        byte[] s = new byte[l];
        ThreadLocalRandom.current().nextBytes(s);
        return Hex.encodeHexString(s);
    }
}
