package edu.ucr.cs.SpatialLSM.impls;

import edu.ucr.cs.SpatialLSM.common.Configuration;
import edu.ucr.cs.SpatialLSM.common.ThroughputLogger;
import edu.ucr.cs.SpatialLSM.common.Utils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicLong;

import edu.ucr.cs.SpatialLSM.apis.IOWoker;
import edu.ucr.cs.SpatialLSM.apis.IOThread;

public class InsertWorker extends IOWoker {

    public InsertWorker(Configuration config, BufferedReader reader, AtomicLong pkid, long startTime) {
        super(config, reader, pkid, config.getBatchSizeInsert(), startTime, "Insert: ");
        if (startTime < 1)
            System.out.println("Insert: size = " + maxOps + ", sleep = " + config.getSleepInsert());
        else
            System.out.println("Insert: duration = " + config.getDuration() + ", sleep = " + config.getSleepInsert());
    }

    @Override
    public Pair<Long, Long> execute() throws InterruptedException {
        reset();
        long  localStartTime = System.currentTimeMillis();
        InsertThread w = new InsertThread(0, maxOps);
        w.task();
        return Pair.of(showProgress(true), System.currentTimeMillis() - localStartTime);
    }

    private class InsertThread extends IOThread {

        private InsertThread(int tid, long totoalOps) {
            super(tid, totoalOps);
        }

        @Override
        public void task() {
            try {
                Socket sock = new Socket(config.getPrivateIP(), config.getFeedPort());
                sock.setKeepAlive(true);
                PrintWriter feedWriter = new PrintWriter(sock.getOutputStream());
                for (long performedOps = 0; getTotoalOps() < 1 || performedOps < getTotoalOps(); performedOps++) {
                    String line = reader.readLine();
                    String[] nums = line.replace("\n", "").split("\t");
                    if (nums.length != 2) {
                        Utils.print("Invalid insert line: " + line + "\n");
                        continue;
                    }
                    double lon = Double.parseDouble(nums[0]);
                    double lat = Double.parseDouble(nums[1]);
                    feedWriter.write(config.newRecord(pkid, lon, lat));
                    showProgress(false);
                    ThroughputLogger.updateStats(1, 0);
                    Utils.print("Insert " + performedOps + ":" + getTotoalOps() + ", lon=" + lon + ", lat=" + lat + "\n");
                    if (startTime > 0 && System.currentTimeMillis() - startTime >= config.getDuration())
                        break;
                    if (config.getSleepInsert() > 0) {
                        long sleepTime;
                        if (startTime > 0 && config.getDuration() > 0)
                            sleepTime = Math.min(config.getSleepInsert(), config.getDuration() * 1000 + startTime - System.currentTimeMillis());
                        else
                            sleepTime = config.getSleepInsert();
                        try {
                            sleep(sleepTime);
                        } catch (InterruptedException e) {
                            break;
                        }
                        if (startTime > 0 && System.currentTimeMillis() - startTime >= config.getDuration())
                            break;
                    }
                }
                feedWriter.close();
                sock.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
