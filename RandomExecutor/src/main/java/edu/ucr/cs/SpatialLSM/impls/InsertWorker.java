package edu.ucr.cs.SpatialLSM.impls;

import edu.ucr.cs.SpatialLSM.common.Configuration;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicLong;
import java.util.ArrayList;
import java.util.List;

import edu.ucr.cs.SpatialLSM.apis.IOWoker;
import edu.ucr.cs.SpatialLSM.apis.IOThread;

public class InsertWorker extends IOWoker {

    public InsertWorker(Configuration config, AtomicLong pkid, long startTime) {
        super(config, pkid, config.getBatchSizeInsert(), startTime, "Insert: ");
        if (startTime < 1)
            System.out.println("Insert: size = " + maxOps + ", threads = " + config.getNumThreadsInsert() + ", sleep = " + config.getSleepInsert());
        else
            System.out.println("Insert: duration = " + config.getDuration() + ", threads = " + config.getNumThreadsInsert() + ", sleep = " + config.getSleepInsert());
    }

    @Override
    public Pair<Long, Long> execute() throws InterruptedException {
        reset();
        long  localStartTime = System.currentTimeMillis();
        if (config.getNumThreadsLoad() == 1) {
            InsertThread w = new InsertThread(0, maxOps);
            w.task();
        } else {
            long batch = (long) Math.ceil((double) maxOps / config.getNumThreadsInsert());
            List<InsertThread> threads = new ArrayList<>();
            for (int i = 0; i < config.getNumThreadsInsert() - 1; i++)
                threads.add(new InsertThread(i + 1, batch));
            threads.add(new InsertThread(config.getNumThreadsInsert(), maxOps - batch * (config.getNumThreadsInsert() - 1)));

            for (InsertThread w : threads)
                w.start();
            for (InsertThread w : threads)
                w.join();

        }
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
                    feedWriter.write(config.newRecord(pkid));
                    showProgress(false);
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
                sock.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
