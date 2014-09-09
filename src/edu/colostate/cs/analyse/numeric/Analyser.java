package edu.colostate.cs.analyse.numeric;

import java.io.File;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: amila
 * Date: 5/28/14
 * Time: 4:13 PM
 * To change this template use File | Settings | File Templates.
 */
public class Analyser {

    private String workingDir;
    private String outDir;

    public Analyser(String workingDir, String outDir) {
        this.workingDir = workingDir;
        this.outDir = outDir;
    }

    private void process() throws IOException {
        File workingDirectory = new File(this.workingDir);
        for (File file : workingDirectory.listFiles()) {
            System.out.println("Processing the record " + file.getName());
            RecordProcessor recordProcessor = new RecordProcessor(file.getName(), this.workingDir, this.outDir, 120 * 60);
            recordProcessor.process();
        }
    }

    public static void main(String[] args) {
        Analyser analyser = new Analyser("data", "result");
        try {
            analyser.process();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
