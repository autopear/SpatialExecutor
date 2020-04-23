package edu.ucr.cs.SpatialLSM.common;

import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class DBConnector {
    private final String serviceURL;
    private CloseableHttpClient client = null;
    private CloseableHttpResponse response = null;
    private InputStream stream = null;
    private InputStreamReader streamReader = null;
    private BufferedReader bufferedReader = null;

    public DBConnector(final String serviceURL) {
        this.serviceURL = serviceURL;
        client = HttpClients.createDefault();
    }

    public String execute(String sql, String err) {
        HttpPost post = new HttpPost(serviceURL);
        post.setHeader("Content-Type", "application/x-www-form-urlencoded");

        List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("statement", sql));
        params.add(new BasicNameValuePair("mode", "immediate"));
        try {
            post.setEntity(new UrlEncodedFormEntity(params));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return "";
        }

        try {
            response = client.execute(post);
        } catch (IOException e) {
            err = e.getMessage();
            closeCurrentResponse();
            return "";
        }

        StatusLine s = response.getStatusLine();
        if (s.getStatusCode() == 200) {
            try {
                stream = response.getEntity().getContent();
            } catch (IOException e) {
                err = e.getMessage();
                closeCurrentResponse();
                return "";
            }

            try {
                streamReader = new InputStreamReader(stream, StandardCharsets.UTF_8);
                bufferedReader = new BufferedReader(streamReader);
                List<String> results = new ArrayList<>();
                String line;
                while ((line = bufferedReader.readLine()) != null)
                    results.add(line.replaceAll("[\r\n]", ""));
                return String.join("\n", results);
            } catch (UnsupportedEncodingException e) {
                err = e.getMessage();
                closeCurrentResponse();
                return "";
            } catch (IOException e) {
                err = e.getMessage();
                return "";
            }
        } else {
            err = s.toString();
            closeCurrentResponse();
            return "";
        }
    }

    private void closeCurrentResponse() {
        if (bufferedReader != null) {
            try {
                bufferedReader.close();
            } catch (IOException ex) {
                // pass
            }
            bufferedReader = null;
        }

        if (streamReader != null) {
            try {
                streamReader.close();
            } catch (IOException ex) {
                // pass
            }
            streamReader = null;
        }

        if (stream != null) {
            try {
                stream.close();
            } catch (IOException ex) {
                // pass
            }
            stream = null;
        }

        if (response != null) {
            try {
                response.close();
            } catch (IOException ex) {
                // pass
            }
            response = null;
        }
    }

    public void close() {
        closeCurrentResponse();
        try {
            client.close();
        } catch (IOException e) {
        }
    }
}
