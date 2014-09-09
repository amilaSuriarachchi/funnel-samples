package edu.colostate.cs.sample;

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
 * Date: 5/22/14
 * Time: 2:35 PM
 * To change this template use File | Settings | File Templates.
 */
public class SubjectDeployer extends InstanceDeployer {

    private ProgressTracker progressTracker;

    public SubjectDeployer(Properties streamingProperties)
            throws CommunicationsException, IOException, MarshallingException,
            DeploymentException {
        initialize(streamingProperties);
    }

    public void prepareInstancesAndDeploy(int numOfOperations)
            throws DatasetException, CommunicationsException,
            DeploymentException, MarshallingException {

        SubjectMap[] operations = new SubjectMap[numOfOperations];
        for (int i = 0; i < numOfOperations; i++) {
            operations[i] = initializeSubjectMap(i);
        }
        progressTracker = deployOperations(operations);
    }

    public SubjectMap initializeSubjectMap(int postFix) throws DatasetException {
        SubjectMap operation = new SubjectMap();
        Properties props = new Properties();
        String value = "(" + postFix + ")";
        String property = "Worker Number";
        props.put(property, value);
        operation.setProcessingDirectives(props);

        DatasetFactory datasetFactory = DatasetFactory.getInstance();
        StreamingAccess streamingAccess = datasetFactory
                .createStreamingDataset("SubjectStreamDatasetID", "Testcase");

        String streamSynopsis = "WorkerStreams/Subject";

        streamingAccess.addInputStream(streamSynopsis,
                StreamingAccess.STRING_SYNOPSIS);

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
            SubjectDeployer deployer = new SubjectDeployer(streamingProperties);
            deployer.prepareInstancesAndDeploy(numOfMaps);

        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

    }

}
