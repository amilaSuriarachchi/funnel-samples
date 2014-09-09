package edu.colostate.cs.analyse.numeric;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: amila
 * Date: 5/28/14
 * Time: 9:08 AM
 * To change this template use File | Settings | File Templates.
 */
public class Interval {

    private double startTime;
    private double min;
    private double max;
    private double mean;
    private double median;
    private double std;

    private double total;

    private List<Double> numbers;

    public Interval(double startTime) {
        this.numbers = new ArrayList<Double>();
        this.startTime = startTime;
    }

    public void addValue(double number) {
        this.total += number;

        for (int i = 0; i < this.numbers.size(); i++) {
            if (this.numbers.get(i) > number) {
                this.numbers.add(i, number);
                return;
            }
        }
        // if the number is greater than every one just return that.
        this.numbers.add(number);
    }

    public void calculateValues() {
        if (this.numbers.size() > 0) {
            this.min = this.numbers.get(0);
            this.max = this.numbers.get(this.numbers.size() - 1);
            this.mean = this.total / this.numbers.size();

            int midIndex = this.numbers.size() / 2;
            if (this.numbers.size() % 2 == 0) {
                this.median = (this.numbers.get(midIndex) + this.numbers.get(midIndex - 1)) / 2;
            } else {
                this.median = this.numbers.get(midIndex);
            }

            double totalMeanSq = 0;
            double meanDeviation = 0;
            for (int i = 0; i < this.numbers.size(); i++) {
                meanDeviation = this.numbers.get(i) - this.mean;
                totalMeanSq += meanDeviation * meanDeviation;
            }
            this.std = Math.sqrt(totalMeanSq / this.numbers.size());
        }

    }

    public void save(BufferedWriter bufferedWriter) throws IOException {
        bufferedWriter.write(this.toString());
        bufferedWriter.newLine();
    }

    @Override
    public String toString() {
        if (this.numbers.size() == 0) {
            return this.startTime + "(0)" + "\t" + "-" + "\t" + "-" + "\t" + "-" + "\t" + "-" + "\t" + "-";
        } else {
            return this.startTime + "(" + this.numbers.size() + ")" + "\t" + this.min + "\t" + this.max + "\t" + this.mean + "\t" + this.median + "\t" + this.std;
        }

    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    public double getMean() {
        return mean;
    }

    public double getMedian() {
        return median;
    }

    public double getStd() {
        return std;
    }

    public List<Double> getNumbers() {
        return numbers;
    }
}
