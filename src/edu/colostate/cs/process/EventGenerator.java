package edu.colostate.cs.process;

import cgl.narada.service.ServiceException;
import ds.granules.exception.CommunicationsException;
import ds.granules.scheduler.StreamingService;
import edu.colostate.cs.analyse.ecg.Record;
import edu.colostate.cs.analyse.ecg.RecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created with IntelliJ IDEA.
 * User: amila
 * Date: 7/10/14
 * Time: 11:25 AM
 * To change this template use File | Settings | File Templates.
 */
public class EventGenerator {

    private String record;
    private String workingDirectory;

    private CountDownLatch latch;

    private EventSender[] eventSenders;

    private long startTime;

    private int MESSAGE_BUFFER_SIZE = 500;

    public EventGenerator(String record,
                          String workingDirectory,
                          int numberOfThreads,
                          int startPoint) throws ServiceException, CommunicationsException {

        this.record = record;
        this.workingDirectory = workingDirectory;

        //initialise the event senders
        this.latch = new CountDownLatch(numberOfThreads);

        this.eventSenders = new EventSender[numberOfThreads];
        for (int i = 0; i < numberOfThreads; i++) {
            this.eventSenders[i] = new EventSender("ecg" + (i + startPoint), this.latch);
            Thread thread = new Thread(this.eventSenders[i]);
            thread.start();
        }
    }

    private void publishEvent(Record[] records) {
        for (EventSender eventSender : this.eventSenders) {
            eventSender.addRecords(records);
        }
    }


    public void sendMessages() {

        try {

            List<String> commands = new ArrayList<String>();
            commands.add("rdsamp");
            commands.add("-r");
            commands.add(this.record);
            commands.add("-p");
//            commands.add("-f");
//            commands.add("1000");
//            commands.add("-t");
//            commands.add("1100");
            commands.add("-c");
            commands.add("-s");
            commands.add("II");

            RecordReader recordReader = null;
            long totalMessages = 0;
            Record[] messageBuffer = new Record[MESSAGE_BUFFER_SIZE];
            int bufferPointer = 0;

            this.startTime = System.currentTimeMillis();

            try {
                recordReader = new RecordReader(commands, this.workingDirectory);
                while (recordReader.hasNext()) {
                    messageBuffer[bufferPointer] = recordReader.next();
                    bufferPointer++;
                    if (bufferPointer == MESSAGE_BUFFER_SIZE){
                        // this means buffer is full.
                        this.publishEvent(messageBuffer);
                        bufferPointer = 0;
                    }
                    totalMessages++;
                    if (totalMessages % 500000 == 0){
                        System.out.println("Number of messages processed " + totalMessages + " time " + System.currentTimeMillis());
                    }
                }
                recordReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void calculateStat(){

        //first finish the threads
        for (EventSender eventSender : this.eventSenders){
            eventSender.setFinish();
        }

        try {
            this.latch.await();
        } catch (InterruptedException e) {}

        long totalTime = System.currentTimeMillis() - this.startTime;

        long totalMessages = 0;
        for (EventSender eventSender : this.eventSenders){
            totalMessages += eventSender.getNumberOfRecords();
        }
        System.out.println("Total messages " + totalMessages);
        System.out.println("Total time " + totalTime);
        System.out.println("Through put " + (totalMessages * 1000.0)/ totalTime);
    }

    public static void main(String[] args) {

        int numOfThreads = Integer.parseInt(args[2]);
        int startPoint = Integer.parseInt(args[3]);
        // initialise the event senders
        try {
            EventGenerator generator =
                    new EventGenerator(args[0], args[1], numOfThreads, startPoint);
            generator.sendMessages();
            generator.calculateStat();
        } catch (ServiceException e) {
            e.printStackTrace();
        } catch (CommunicationsException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        // calculate the total;

    }
}
