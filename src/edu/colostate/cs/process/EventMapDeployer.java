package edu.colostate.cs.process;

import ds.granules.dataset.DatasetCollection;
import ds.granules.dataset.DatasetException;
import ds.granules.dataset.DatasetFactory;
import ds.granules.dataset.StreamingAccess;
import ds.granules.exception.CommunicationsException;
import ds.granules.exception.DeploymentException;
import ds.granules.exception.MarshallingException;
import ds.granules.operation.InstanceDeployer;
import ds.granules.operation.ProgressTracker;

import java.io.IOException;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: amila
 * Date: 7/10/14
 * Time: 12:11 PM
 * To change this template use File | Settings | File Templates.
 */
public class EventMapDeployer extends InstanceDeployer {

    private ProgressTracker progressTracker;

    public EventMapDeployer(Properties streamingProperties)
            throws CommunicationsException, IOException, MarshallingException,
            DeploymentException {
        initialize(streamingProperties);
    }

    public void prepareInstancesAndDeploy(int numberOfOperations)
            throws DatasetException, CommunicationsException,
            DeploymentException, MarshallingException {

        EventMap[] operations = new EventMap[numberOfOperations];
        for (int i = 0; i < numberOfOperations; i++) {
            operations[i] = initializeSubjectMap("ecg" + i);
        }
        progressTracker = deployOperations(operations);

    }

    public EventMap initializeSubjectMap(String streamName) throws DatasetException {
        EventMap operation = new EventMap();
        Properties props = new Properties();
        props.put("dataSetID", streamName);
        operation.setProcessingDirectives(props);

        DatasetFactory datasetFactory = DatasetFactory.getInstance();
        StreamingAccess streamingAccess = datasetFactory
                .createStreamingDataset(streamName, "Testcase");

        streamingAccess.addInputStream(streamName, StreamingAccess.STRING_SYNOPSIS);

        DatasetCollection datasetCollection = operation.getDatasetCollection();

        datasetCollection.addDataset(streamingAccess);
        operation.setDatasetCollection(datasetCollection);
        operation.setAsExecuteWhenDataAvailable();

        return operation;
    }


    public static void main(String[] args) {

        String hostname = args[0];
        String portNumber = args[1];
        int numOfMaps = Integer.parseInt(args[2]);


        Properties streamingProperties = new Properties();
        streamingProperties.put("hostname", hostname);
        streamingProperties.put("portnum", portNumber);

        try {
            EventMapDeployer deployer = new EventMapDeployer(streamingProperties);
            deployer.prepareInstancesAndDeploy(numOfMaps);

        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

    }
}
