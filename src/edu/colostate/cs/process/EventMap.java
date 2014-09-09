package edu.colostate.cs.process;

import cgl.narada.event.NBEvent;
import ds.granules.dataset.Dataset;
import ds.granules.dataset.DatasetCollection;
import ds.granules.dataset.DatasetException;
import ds.granules.dataset.StreamingAccess;
import ds.granules.operation.MapReduceBase;
import ds.granules.operation.MapReduceException;
import ds.granules.operation.ProcessingException;
import edu.colostate.cs.analyse.ecg.Record;
import edu.colostate.cs.analyse.ecg.Tompikens;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * this class receives the events from granulus and process them.
 */
public class EventMap extends MapReduceBase {

    private Tompikens tompikens;

    public EventMap() {
        setDomain("Generic");
        setVersionInformation(200);
        setOperationIdentifier("ABCDEFGHIJKLMN");
        this.tompikens = new Tompikens();

    }

    @Override
    public void execute() throws ProcessingException {

        String dataSetID = (String) getProcessingDirectives().get("dataSetID");
        DatasetCollection datasetCollection = getDatasetCollection();
        StreamingAccess access = null;
        try {

            Dataset dataset = datasetCollection.getDataset(dataSetID);

            if (dataset.getDatasetType() == Dataset.STREAMS) {
                access = (StreamingAccess) dataset;
            } else {
                System.out.println("Incorrect datatset: "
                        + dataset.getDatasetType() + " initialized. Returning ...");
                return;
            }

            if (access != null) {

                while (dataset.isDataAvailable()) {
                    NBEvent nbEvent = access.getStreamingData();
                    processStreamPacket(nbEvent);
                }

            } else {
                System.out.println("\nThe INPUT Dataset is NULL!\n");
            }

        } catch (DatasetException e) {
            e.printStackTrace();

        } catch (MapReduceException e) {
            System.out.println("Problems writing Results: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void processStreamPacket(NBEvent nbEvent) throws MapReduceException {

        DataInput dataInput = new DataInputStream(new ByteArrayInputStream(nbEvent.getContentPayload()));
        try {
            Record record = new Record();
            record.parse(dataInput);
            this.tompikens.bandPass(record);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
