package edu.colostate.cs.sample;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: amila
 * Date: 5/26/14
 * Time: 6:29 PM
 * To change this template use File | Settings | File Templates.
 */
public class RecordReader implements Iterator<Map<String, String>> {

    private BufferedReader bufferedReader;
    private Process process;
    private String currentLine;
    private String[] fieldNames;

    public RecordReader(List<String> commands, String workingDirectory) throws IOException {
        ProcessBuilder processBuilder = new ProcessBuilder(commands);
        processBuilder.directory(new File(workingDirectory));
        this.process = processBuilder.start();

        this.bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String readLine = this.bufferedReader.readLine();
        // first line is the header line
        this.fieldNames = readLine.split("\t");
        for (int i = 0; i < this.fieldNames.length; i++) {
            this.fieldNames[i] = this.fieldNames[i].trim();
        }
        // omit the second line
        this.bufferedReader.readLine();

    }

    @Override
    public boolean hasNext() {
        try {
            this.currentLine = this.bufferedReader.readLine();
            return this.currentLine != null;
        } catch (IOException e) {
            //if we can not read the next line still better to send false
        }
        return false;
    }

    @Override
    public Map<String, String> next() {
        Map<String, String> event = new HashMap<String, String>();
        String[] fieldValues = this.currentLine.split("\t");
        for (int i = 0; i < this.fieldNames.length; i++) {
            event.put(this.fieldNames[i], fieldValues[i].trim());
        }
        return event;
    }

    @Override
    public void remove() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void close() {
        try {
            this.bufferedReader.close();
            this.process.destroy();
        } catch (IOException e) {
            // can not do any thing.
        }
    }
}
