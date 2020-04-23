package edu.ucr.cs.SpatialLSM.impls;

import edu.ucr.cs.SpatialLSM.common.Configuration;
import edu.ucr.cs.SpatialLSM.common.Utils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicLong;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import edu.ucr.cs.SpatialLSM.apis.IOThread;
import edu.ucr.cs.SpatialLSM.apis.IOWoker;

public class LoadWorker extends IOWoker {
    
    public LoadWorker(Configuration config, GZIPInputStream gis, AtomicLong pkid, long startTime) {
        super(config, gis, pkid, config.getSizeLoad(), startTime, "Load:   ");
        if (startTime < 1)
            System.out.println("Load: size = " + maxOps + ", sleep = " + config.getSleepLoad());
        else
            System.out.println("Load: duration = " + config.getDuration() + ", sleep = " + config.getSleepLoad());
    }

    public Pair<Long, Long> execute() throws InterruptedException {
        reset();
        long localStartTime = System.currentTimeMillis();
        LoadThreadWorker w = new LoadThreadWorker(0, maxOps);
        w.task();
        return Pair.of(showProgress(true), System.currentTimeMillis() - localStartTime);
    }

    private class LoadThreadWorker extends IOThread {

        private final byte[] numBuf = new byte[Double.BYTES * 2];

        private LoadThreadWorker(int tid, long totoalOps) {
            super(tid, totoalOps);
        }

        @Override
        public void task() {
            try {
                Socket sock = new Socket(config.getPrivateIP(), config.getFeedPort());
                sock.setKeepAlive(true);
                PrintWriter feedWriter = new PrintWriter(sock.getOutputStream());
                for (long performedOps = 0; getTotoalOps() < 1 || performedOps < getTotoalOps(); performedOps++) {
                    gzis.read(numBuf);
                    double lon = Utils.bytes2double(numBuf, 0, Double.BYTES);
                    double lat = Utils.bytes2double(numBuf, Double.BYTES, Double.BYTES);
                    feedWriter.write(config.newRecord(pkid, lon, lat));
                    showProgress(false);
                    if (getTotoalOps() < 1 && startTime > 0 && System.currentTimeMillis() - startTime >= config.getDuration())
                        break;
                    if (config.getSleepLoad() > 0) {
                        long sleepTime;
                        if (getTotoalOps() < 1 && startTime > 0 && config.getDuration() > 0)
                            sleepTime = Math.min(config.getSleepLoad(), config.getDuration() * 1000 + startTime - System.currentTimeMillis());
                        else
                            sleepTime = config.getSleepLoad();
                        try {
                            sleep(sleepTime);
                        } catch (InterruptedException e) {
                            break;
                        }
                        if (getTotoalOps() < 1 && startTime > 0 && System.currentTimeMillis() - startTime >= config.getDuration())
                            break;
                    }
                }
                sock.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
