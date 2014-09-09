package edu.colostate.cs.process;

import cgl.narada.event.NBEvent;
import cgl.narada.event.NBEventException;
import cgl.narada.event.TemplateProfileAndSynopsisTypes;
import cgl.narada.service.ServiceException;
import cgl.narada.service.client.ClientService;
import cgl.narada.service.client.EventProducer;
import cgl.narada.service.client.SessionService;
import ds.granules.exception.CommunicationsException;
import ds.granules.scheduler.StreamingService;
import edu.colostate.cs.analyse.ecg.Record;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * Created with IntelliJ IDEA.
 * User: amila
 * Date: 7/24/14
 * Time: 8:55 AM
 * To change this template use File | Settings | File Templates.
 */
public class EventSender implements Runnable {

    public static final int MAX_SIZE = 1000;

    private Queue<Record> messages;
    private boolean isFinished;


    private long numberOfRecords = 0;

    private EventProducer producer;
    private String stream;
    private CountDownLatch latch;

    public EventSender(String stream,
                       CountDownLatch latch) throws ServiceException, CommunicationsException {


        this.messages = new LinkedList<Record>();
        this.isFinished = false;
        this.stream = stream;
        this.latch = latch;
        StreamingService.getInstance().initialize(null, 12345, null, null);
        this.initializeProducer();

    }

    public void initializeProducer() throws ServiceException, CommunicationsException {
        this.producer = StreamingService.getInstance().createProducer();

        this.producer.generateEventIdentifier(true);
        this.producer.setTemplateId(12345);
        this.producer.setDisableTimestamp(false);

        this.producer.setSuppressRedistributionToSource(true);
    }


    /**
     * Closes the connection to the broker.
     *
     * @throws ServiceException
     */
    public void closeBrokerConnection() throws ServiceException {
        try {
            /** Make sure that the transmission tracker is shutdown gracefully. */
            Thread.sleep(100);
        } catch (InterruptedException e) {
            System.out.println("Problems sleeping while waiting for transfer completion");
        }
    }




    public synchronized void addRecord(Record record) {
        if (this.messages.size() == MAX_SIZE) {
            try {
                this.wait();
            } catch (InterruptedException e) {
            }
            addRecord(record);
        } else {
            this.messages.add(record);
            this.notify();
        }

    }

    public synchronized void addRecords(Record[] records) {
        if (this.messages.size() >= MAX_SIZE) {
            try {
                this.wait();
            } catch (InterruptedException e) {
            }
            addRecords(records);
        } else {
            for (Record record : records) {
                this.messages.add(record);
            }
            this.notify();
        }

    }

    public synchronized Record getRecord() {

        Record record = this.messages.poll();
        while ((record == null) && !this.isFinished) {
            try {
                this.wait();
            } catch (InterruptedException e) {
            }
            record = this.messages.poll();
        }
        this.notify();
        return record;

    }

    public synchronized void setFinish() {
        this.isFinished = true;
        this.notify();
    }

    public long getNumberOfRecords() {
        return this.numberOfRecords;
    }

    public void publishEvent(Record event) throws ServiceException, NBEventException, IOException {

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutput dataOutput = new DataOutputStream(byteArrayOutputStream);
        event.serialise(dataOutput);
        byteArrayOutputStream.flush();

        NBEvent nbEvent = producer.generateEvent(TemplateProfileAndSynopsisTypes.STRING,
                this.stream, byteArrayOutputStream.toByteArray());

        producer.publishEvent(nbEvent);
    }


    @Override
    public void run() {

        Record record = null;
        // record will be thread executions is over.
        while ((record = getRecord()) != null) {
            try {
                this.publishEvent(record);
            } catch (ServiceException e) {
                e.printStackTrace();
            } catch (NBEventException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            this.numberOfRecords++;
        }

        try {
            this.closeBrokerConnection();
        } catch (ServiceException e) {
            e.printStackTrace();
        }
        this.latch.countDown();
    }
}
