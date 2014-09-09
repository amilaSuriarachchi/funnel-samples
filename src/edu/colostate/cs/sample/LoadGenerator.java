package edu.colostate.cs.sample;

import cgl.narada.event.EventProperties;
import cgl.narada.event.NBEvent;
import cgl.narada.event.NBEventException;
import cgl.narada.event.TemplateProfileAndSynopsisTypes;
import cgl.narada.matching.Profile;
import cgl.narada.service.ServiceException;
import cgl.narada.service.client.*;
import ds.granules.exception.CommunicationsException;
import ds.granules.exception.MarshallingException;
import ds.granules.results.Results;
import ds.granules.results.ResultsFactory;
import ds.granules.scheduler.StreamingService;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: amila
 * Date: 5/22/14
 * Time: 3:13 PM
 * To change this template use File | Settings | File Templates.
 */
public class LoadGenerator {

    private String moduleName = "SimpleClient: ";
    private EventProducer producer;
    private String topic = "WorkerStreams/Subject";

    public void initializeProducer() throws ServiceException, CommunicationsException {
        producer = StreamingService.getInstance().createProducer();

        producer.generateEventIdentifier(true);
        producer.setTemplateId(12345);
        producer.setDisableTimestamp(false);

        producer.setSuppressRedistributionToSource(true);
    }

    public void closeBrokerConnection() throws ServiceException {
        unsubscribe();
        try {
            /** Make sure that the transmission tracker is shutdown gracefully. */
            Thread.sleep(100);
        } catch (InterruptedException e) {
            System.out.println(moduleName
                    + "Problems sleeping while waiting for transfer completion");
        }
    }

    public void unsubscribe() throws ServiceException {
//        consumer.unSubscribe(profile);
    }

    public void publishEvent(byte[] transferBytes) throws ServiceException,
            NBEventException {
        NBEvent nbEvent =
                producer.generateEvent(TemplateProfileAndSynopsisTypes.STRING,
                        topic, transferBytes);
        producer.publishEvent(nbEvent);
    }


    public void onEvent(NBEvent nbEvent) {
        String synopsis = (String) nbEvent.getContentSynopsis();
        System.out.println(moduleName + "Received results on {" + synopsis
                + "}");

        ResultsFactory resultsFactory = ResultsFactory.getInstance();
        try {
            Results results =
                    resultsFactory.getResults(nbEvent.getContentPayload());
            System.out.println(new String(results.getResultPayload()));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (MarshallingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public void publishEvent(Map<String, String> event) throws ServiceException, NBEventException, IOException {

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutput dataOutput = new DataOutputStream(byteArrayOutputStream);
        dataOutput.writeInt(event.size());
        for (String key : event.keySet()) {
            dataOutput.writeUTF(key);
            dataOutput.writeUTF(event.get(key));
        }
        byteArrayOutputStream.flush();

        publishEvent(byteArrayOutputStream.toByteArray());

    }


    public static void main(String[] args) {

        try {
            LoadGenerator client = new LoadGenerator();
            /** Initializing communications using specified transport */
            StreamingService.getInstance().initialize(null, 12345, null, null);
            client.initializeProducer();

            //use a record reader to send these events to the processing element
            List<String> commands = new ArrayList<String>();
            commands.add("rdsamp");
            commands.add("-r");
            commands.add(args[0]);
            commands.add("-p");
            commands.add("-v");
            commands.add("-t");
            commands.add("10");

            RecordReader recordReader = new RecordReader(commands, args[1]);
            while (recordReader.hasNext()) {
                client.publishEvent(recordReader.next());
            }

            recordReader.close();
            client.closeBrokerConnection();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
