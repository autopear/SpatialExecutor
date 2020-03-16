package edu.ucr.cs.SpatialLSM.common;

import java.io.*;
import java.text.NumberFormat;

public class Utils {

    public static String getCommandOutput(final String cmd) {
        try {
            ProcessBuilder builder = new ProcessBuilder("bash", "-c", cmd);
            builder.redirectErrorStream(true);
            Process process = builder.start();
            InputStream is = process.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.replaceAll("\r", "\n");
                if (line.endsWith("\n"))
                    sb.append(line);
                else
                    sb.append(line).append("\n");
            }
            return sb.toString();
        } catch (IOException e) {
            e.printStackTrace();
            return "";
        }
    }

    public static int runCommand(final String cmd) {
        try {
            ProcessBuilder builder = new ProcessBuilder("bash", "-c", cmd);
            builder.redirectErrorStream(true);
            Process process = builder.start();
            InputStream is = process.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.replaceAll("[\r\n]", "");
                System.out.println(line);
            }
            return 0;
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
    }

    public static int runRemoteCommand(final String node, final String cmd) {
        try {
            ProcessBuilder builder = new ProcessBuilder("bash", "-c", "ssh " + node + " \"" + cmd + "\"");
            builder.redirectErrorStream(true);
            Process process = builder.start();
            InputStream is = process.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.replaceAll("[\r\n]", "");
                System.out.println(line);
            }
            return 0;
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
    }

    public static String formatPath(final String path) {
        File f = new File(path);
        return f.getAbsolutePath();
    }

    public synchronized static void print(String msg) {
        System.out.print(msg);
    }

    public static String num2str(long n) {
        return NumberFormat.getNumberInstance().format(n);
    }

    public static String durationToString(long secs) {
        if (secs < 60)
            return secs + " s"; // x s
        long s = secs % 60;
        String tStr = s == 0 ? "" : " " + s + " s";
        long tm = (secs - s) / 60;
        if (tm < 60)
            return tm + " m" + tStr; // y m [x s]
        long m = tm % 60;
        tStr = m == 0 ? tStr : m + " m" + tStr;
        long th = (tm - m) / 60;
        if (th < 24)
            return th + " h" + tStr; // z h [y m] [x s]
        long h = th % 24;
        tStr =  h == 0 ? tStr : h + " h" + tStr;
        long d = (th - h) / 24;
        return d + " d" + tStr; // w d[ z h][ y m][ x s]
    }
}
