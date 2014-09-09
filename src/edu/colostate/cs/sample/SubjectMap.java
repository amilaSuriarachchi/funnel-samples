package edu.colostate.cs.sample;

import cgl.narada.event.NBEvent;
import ds.granules.dataset.Dataset;
import ds.granules.dataset.DatasetCollection;
import ds.granules.dataset.DatasetException;
import ds.granules.dataset.StreamingAccess;
import ds.granules.operation.MapReduceBase;
import ds.granules.operation.MapReduceException;
import ds.granules.operation.ProcessingException;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: amila
 * Date: 5/22/14
 * Time: 2:52 PM
 * To change this template use File | Settings | File Templates.
 */
public class SubjectMap extends MapReduceBase {

    public SubjectMap() {
        setDomain("Generic");
        setVersionInformation(200);
        setOperationIdentifier("ABCDEFGHIJKLMN");
    }

    @Override
    public void execute() throws ProcessingException {

        String datasetIdentifier = "SubjectStreamDatasetID";
        DatasetCollection datasetCollection = getDatasetCollection();
        StreamingAccess access = null;
        try {
            Dataset dataset = datasetCollection.getDataset(datasetIdentifier);

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
            int fields = dataInput.readInt();
            for (int i = 0; i < fields; i++) {
                System.out.print(dataInput.readUTF() + ">" + dataInput.readUTF() + ",");
            }
            System.out.println("");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
