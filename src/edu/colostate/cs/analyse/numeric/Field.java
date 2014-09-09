package edu.colostate.cs.analyse.numeric;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: amila
 * Date: 5/28/14
 * Time: 10:22 AM
 * To change this template use File | Settings | File Templates.
 */
public class Field {

    private String field;
    private int intervalSize;
    private List<Interval> intervals;
    private Interval currentInterval;

    public Field(int intervalSize, String field) {
        this.intervalSize = intervalSize;
        this.field = field;
        this.intervals = new ArrayList<Interval>();

    }

    public void addValue(double time, String value){
        if (time % this.intervalSize == 0){
            if (this.currentInterval != null){
                //calculate the values after interval is finished
                this.currentInterval.calculateValues();
            }
            // create a new interval and add the to list
            this.currentInterval = new Interval(time);
            this.intervals.add(this.currentInterval);
        }
        if (!value.equals("-")){
            this.currentInterval.addValue(Double.parseDouble(value));
        }

    }

    public void finalise(){
        if (this.currentInterval != null){
            // this is to calculate the values for last interval
            this.currentInterval.calculateValues();
        }
    }

    public void save(BufferedWriter bufferedWriter) throws IOException {
         for (Interval interval : this.intervals){
             interval.save(bufferedWriter);
         }
    }

    @Override
    public String toString() {
        StringBuffer stringBuffer = new StringBuffer();
        for (Interval interval : this.intervals){
            stringBuffer.append(interval.toString() + "\n");
        }
        return stringBuffer.toString();
    }

    public String getField() {
        return field;
    }
}
