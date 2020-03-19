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
    private int minExp = -1;
    private int maxExp = -1;
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
            feedPort = Integer.parseInt(jsonObject.get("feed_port").toString());
            asterixDBPath = Utils.formatPath(jsonObject.get("asterixdb").toString());
            startScript = Utils.formatPath(asterixDBPath + "/opt/local/bin/start-sample-cluster.sh");
            stopScript = Utils.formatPath(asterixDBPath + "/opt/local/bin/stop-sample-cluster.sh");
            resetDBPath = Utils.formatPath(jsonObject.get("reset_db").toString());
            logParserPath = Utils.formatPath(jsonObject.get("log_parser").toString());
            logsDir = Utils.formatPath(jsonObject.get("logs_dir").toString());
            taskName = jsonObject.get("task").toString().trim();
            readLogPath = Utils.formatPath(logsDir + "/" + taskName + ".read.tsv");
            rtreePolicy = jsonObject.get("rtree").toString().replace("[\r\n]", " ").replace("\"", "\\\"");
            while (rtreePolicy.contains("  "))
                rtreePolicy = rtreePolicy.replace("  ", " ");
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
            if (jsonObject.containsKey("exp")) {
                JSONArray exps = (JSONArray) jsonObject.get("exp");
                minExp = Integer.parseInt(exps.get(0).toString());
                maxExp = Integer.parseInt(exps.get(1).toString());
            }
            if (jsonObject.containsKey("sleep_load"))
                loadSleep = Long.parseLong(jsonObject.get("sleep_load").toString());
            if (jsonObject.containsKey("sleep_insert"))
                insertSleep = Long.parseLong(jsonObject.get("sleep_insert").toString());
            if (jsonObject.containsKey("sleep_read"))
                readSleep = Long.parseLong(jsonObject.get("sleep_read").toString());
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

    public int getMinExp() {
        return minExp;
    }

    public int getMaxExp() {
        return maxExp;
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
