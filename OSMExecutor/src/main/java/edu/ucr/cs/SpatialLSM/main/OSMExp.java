package edu.ucr.cs.SpatialLSM.main;

import edu.ucr.cs.SpatialLSM.common.Configuration;
import edu.ucr.cs.SpatialLSM.common.DBConnector;
import edu.ucr.cs.SpatialLSM.common.Utils;
import edu.ucr.cs.SpatialLSM.impls.InsertWorker;
import edu.ucr.cs.SpatialLSM.impls.LoadWorker;
import edu.ucr.cs.SpatialLSM.impls.ReadWorker;
import org.apache.commons.lang3.tuple.Pair;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;
import java.io.File;
import java.util.zip.GZIPInputStream;

public class OSMExp {

    public static void main(String[] args) throws InterruptedException {
        if (args.length != 3) {
            System.out.println("SpatialExp TASK INIT_SCRIPT NODE");
            System.exit(0);
        }

        String taskSetting = args[0].toUpperCase();
        if (taskSetting.compareTo("L") != 0
                && taskSetting.compareTo("LM") != 0
                && taskSetting.compareTo("LR") != 0
                && taskSetting.compareTo("LIR") != 0
                && taskSetting.compareTo("M") != 0
                && taskSetting.compareTo("IR") != 0) {
            System.out.println("TASK can be only one of L, LM, LR, LIR, M and IR.");
            System.exit(-1);
        }

        String initScript = "";
        try {
            initScript = new String(Files.readAllBytes(Paths.get(Utils.formatPath(args[1]))));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        Configuration config = new Configuration(Utils.formatPath(args[2]));
        if (!config.isValid()) {
            System.out.println("Configuration is invalid.");
            System.exit(-1);
        }

        initScript = initScript.replace("\t", "").replace("  ", "");
        initScript = initScript.replace("RTREE_REPLACE", config.getRtreePolicy());

        if (config.isLocalhost()) {
            Utils.runCommand(config.stopAsterixDBPath() + " -f");
            Utils.runCommand(config.getResetDBPath());
        } else {
            Utils.runRemoteCommand(config.getNodeName(), "bash " + config.stopAsterixDBPath() + " -f");
            Utils.runRemoteCommand(config.getNodeName(), "bash " + config.getResetDBPath());
        }
        File readLogFile = new File(config.getReadLogPath());
        if (readLogFile.exists())
            readLogFile.delete();
        if (config.isLocalhost())
            Utils.runCommand(config.startAsterixDBPath());
        else
            Utils.runRemoteCommand(config.getNodeName(), "bash " + config.startAsterixDBPath());

        DBConnector connector = new DBConnector("http://" + config.getPrivateIP() + ":19002/query/service");
        String sqlErr = "";
        if (connector.execute(initScript, sqlErr).isEmpty()) {
            connector.close();
            System.out.println("Error creating database: " + sqlErr);
            System.exit(-1);
        }

        if (connector.execute("USE Level_Spatial;START FEED Spatial_Feed;", sqlErr).isEmpty()) {
            connector.close();
            System.out.println("Failed to start feed: " + sqlErr);
            System.exit(-1);
        }

        BufferedWriter taskWriter = null;
        try {
            taskWriter = new BufferedWriter(new FileWriter(Utils.formatPath(config.getLogsDir() + "/" + config.getTaskName() + ".task.log"), false));
            taskWriter.write(taskSetting + "\n");
            taskWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        AtomicLong pkid = new AtomicLong(0);

        File writeFile = new File(config.getWriteDataPath());
        File readFile = new File(config.getReadDataPath());
        if ((taskSetting.contains("R") || taskSetting.contains("M")) && !readFile.exists()) {
            System.out.println("Read data file not found: " + config.getReadDataPath());
            System.exit(-1);
        }

        try (FileInputStream wfis = new FileInputStream(writeFile);
             FileInputStream rfis = new FileInputStream(readFile);
             GZIPInputStream wgis = new GZIPInputStream(wfis);
             GZIPInputStream rgis = new GZIPInputStream(rfis)) {
            // Has a pre load phase
            if (taskSetting.startsWith("L")) {
                long maxOps = config.getSizeLoad();
                long duration = config.getDuration();
                if (maxOps < 1 && duration < 1) {
                    System.out.println("At least one of size_load and duration must be greater than 1.");
                    connector.close();
                    System.exit(-1);
                }
                LoadWorker lw = new LoadWorker(config, wgis, pkid, maxOps < 1 ? System.currentTimeMillis() : -1);
                Pair<Long, Long> loadRes = lw.execute();
                try {
                    taskWriter.write("L\t0\t" + loadRes.getLeft() + "\t" + loadRes.getRight() + "\n");
                    taskWriter.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                    connector.close();
                    System.exit(-1);
                }
            }

            long numInserts = 0;
            long numReads = 0;

            // Has a phase of mix of inserts / reads
            if (taskSetting.endsWith("M")) {
                long maxInsertOps = config.getBatchSizeInsert();
                long maxReadOps = config.getBatchSizeRead();
                long duration = config.getDuration();
                if (duration < 1 && (maxInsertOps < 1 || maxReadOps < 1)) {
                    System.out.println("Both batch_insert and batch_reads must be greater than 1 when duration is unset.");
                    connector.close();
                    System.exit(-1);
                }
                long startTime = System.currentTimeMillis();
                InsertWorker iw = new InsertWorker(config, wgis, pkid, startTime);
                ReadWorker rw = new ReadWorker(config, rgis, pkid, startTime);
                rw.clearTmpFiles();
                iw.start();
                rw.start();
                iw.join();
                rw.join();
                System.out.println("Generating " + config.getReadLogPath());
                rw.sortMergeTmpFiles();
                Pair<Long, Long> insertRes = iw.getResult();
                Pair<Long, Long> readRes = rw.getResult();
                try {
                    taskWriter.write("I\t" + (++numInserts) + "\t" + insertRes.getLeft() + "\t" + insertRes.getRight() + "\n");
                    taskWriter.write("R\t" + (++numReads) + "\t" + readRes.getLeft() + "\t" + readRes.getRight() + "\n");
                    taskWriter.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                    connector.close();
                    System.exit(-1);
                }
            }

            // Pure reads
            if (taskSetting.compareTo("LR") == 0) {
                long maxOps = config.getBatchSizeRead();
                long duration = config.getDuration();
                if (maxOps < 1 && duration < 1) {
                    System.out.println("At least one of batch_read and duration must be greater than 1.");
                    connector.close();
                    System.exit(-1);
                }

                long startTime = System.currentTimeMillis();
                ReadWorker rw = new ReadWorker(config, rgis, pkid, maxOps < 1 ? startTime : -1);
                rw.clearTmpFiles();
                Pair<Long, Long> readRes = rw.execute();
                System.out.println("Generating " + config.getReadLogPath());
                rw.sortMergeTmpFiles();
                try {
                    taskWriter.write("R\t" + (++numReads) + "\t" + readRes.getLeft() + "\t" + readRes.getRight() + "\n");
                    taskWriter.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                    connector.close();
                    System.exit(-1);
                }
                System.out.println("Elapsed " + Utils.durationToString(Math.round((double)(System.currentTimeMillis() - startTime) / 1000)));
            }

            // Interleaved of inserts / reads
            if (taskSetting.compareTo("IR") == 0 || taskSetting.compareTo("LIR") == 0) {
                long startTime = System.currentTimeMillis();
                InsertWorker iw = new InsertWorker(config, wgis, pkid, config.getNumBatchInsert() < 1 ? startTime : -1);
                ReadWorker rw = new ReadWorker(config, rgis, pkid, config.getNumBatchRead() < 1 ? startTime : -1);
                rw.clearTmpFiles();
                while (true) {
                    if (config.getNumBatchInsert() < 1 || numInserts < config.getNumBatchInsert()) {
                        Pair<Long, Long> insertRes = iw.execute();
                        try {
                            taskWriter.write("I\t" + (++numInserts) + "\t" + insertRes.getLeft() + "\t" + insertRes.getRight() + "\n");
                            taskWriter.flush();
                        } catch (IOException e) {
                            e.printStackTrace();
                            connector.close();
                            System.exit(-1);
                        }
                    }
                    if ((config.getNumBatchInsert() > 0 && numInserts == config.getNumBatchInsert() &&
                            config.getNumBatchRead() > 0 && numReads == config.getNumBatchRead()) ||
                            (config.getDuration() > 0 && System.currentTimeMillis() - startTime >= config.getDuration()))
                        break;
                    if (config.getNumBatchRead() < 1 || numReads < config.getNumBatchRead()) {
                        Pair<Long, Long> readRes = rw.execute();
                        try {
                            taskWriter.write("R\t" + (++numReads) + "\t" + readRes.getLeft() + "\t" + readRes.getRight() + "\n");
                            taskWriter.flush();
                        } catch (IOException e) {
                            e.printStackTrace();
                            connector.close();
                            System.exit(-1);
                        }
                    }
                    if ((config.getNumBatchInsert() > 0 && numInserts == config.getNumBatchInsert() &&
                            config.getNumBatchRead() > 0 && numReads == config.getNumBatchRead()) ||
                            (config.getDuration() > 0 && System.currentTimeMillis() - startTime >= config.getDuration()))
                        break;
                }
                System.out.println("Generating " + config.getReadLogPath());
                rw.sortMergeTmpFiles();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            taskWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
            connector.close();
            System.exit(-1);
        }

        if (connector.execute("USE Level_Spatial;STOP FEED Spatial_Feed;", sqlErr).isEmpty())
            System.out.println("Failed to stop feed: " + sqlErr);
        connector.close();
        if (config.isLocalhost())
            Utils.runCommand(config.stopAsterixDBPath());
        else
            Utils.runRemoteCommand(config.getNodeName(), "bash " + config.stopAsterixDBPath());

        System.out.println("Getting AsterixDB logs...");
        if (config.isLocalhost()) {
            File pythonBin = new File("/usr/local/bin/python3");
            if (pythonBin.exists())
                Utils.runCommand("/usr/local/bin/python3 " + config.getLogParserPath() + " " + config.getTaskName());
            else {
                pythonBin = new File("/usr/bin/python3");
                if (pythonBin.exists())
                    Utils.runCommand("/usr/bin/python3 " + config.getLogParserPath() + " " + config.getTaskName());
                else
                    System.out.println("No python3 interpreter.");
            }
        } else {
            Utils.runRemoteCommand(config.getNodeName(), "/usr/bin/python3 " + config.getLogParserPath() + " " + config.getTaskName());
            Utils.runCommand("scp " + config.getNodeName() + ":/home/ubuntu/level-lsm/logs/" + config.getTaskName() + ".zip " + config.getLogsDir());
        }

        String zipPath = Utils.formatPath(config.getLogsDir() + "/" + config.getTaskName() + ".zip");
        if (new File(zipPath).exists()) {
            String[] files = {
                    Utils.formatPath(config.getLogsDir() + "/" + config.getTaskName() + ".task.log"),
                    Utils.formatPath(config.getLogsDir() + "/" + config.getTaskName() + ".read.tsv")
            };
            for (String f : files) {
                File file = new File(f);
                if (file.exists()) {
                    Utils.runCommand("zip -ju " + zipPath + " " + f);
                    file.delete();
                }
            }
        } else
            System.out.println("Failed to get " + config.getTaskName() + ".zip");
        System.out.println("Done");
        System.exit(0);
    }
}