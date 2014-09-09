package edu.colostate.cs.analyse.numeric;

import edu.colostate.cs.sample.RecordReader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: amila
 * Date: 5/28/14
 * Time: 12:25 PM
 * To change this template use File | Settings | File Templates.
 */
public class RecordProcessor {

    private String record;
    private String workingDir;
    private String outputDir;
    private Map<String, Field> fieldMap;
    private int intervalSize;

    public RecordProcessor(String record, String workingDir, String outputDir, int intervalSize) {
        this.record = record;
        this.workingDir = workingDir;
        this.outputDir = outputDir;
        this.intervalSize = intervalSize;
        this.fieldMap = new HashMap<String, Field>();
    }

    public void process() throws IOException {
        // create a record reader for this record
        List<String> commands = new ArrayList<String>();
        commands.add("rdsamp");
        commands.add("-r");
        commands.add(this.record + File.separator + this.record + "n");
        commands.add("-p");
        commands.add("-v");

        RecordReader recordReader = new RecordReader(commands, this.workingDir);
        while (recordReader.hasNext()) {
            Map<String, String> event = recordReader.next();
            double elapsedTime = Double.parseDouble(event.remove("Elapsed time"));
            for (String key : event.keySet()) {
                String value = event.get(key);
                addValueToField(key, elapsedTime, value);
            }
        }

        // finalise the reading and print values
        for (Map.Entry<String, Field> entry : this.fieldMap.entrySet()) {
            Field field = entry.getValue();
            field.finalise();
            String fileName = this.outputDir + File.separator + this.record + File.separator + field.getField() + ".txt";
            File file = new File(this.outputDir + File.separator + this.record);
            file.mkdir();
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(fileName));
            field.save(bufferedWriter);
            bufferedWriter.flush();
            bufferedWriter.close();
        }
        recordReader.close();

    }

    private void addValueToField(String field, double time, String value) {
        if (fieldMap.get(field) == null) {
            fieldMap.put(field, new Field(this.intervalSize, field));
        }
        fieldMap.get(field).addValue(time, value);
    }

}
