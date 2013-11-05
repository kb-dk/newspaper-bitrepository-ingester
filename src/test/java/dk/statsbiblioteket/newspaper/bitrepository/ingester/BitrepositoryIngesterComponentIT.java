package dk.statsbiblioteket.newspaper.bitrepository.ingester;

import java.io.FileInputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import dk.statsbiblioteket.medieplatform.autonomous.Batch;
import dk.statsbiblioteket.medieplatform.autonomous.ResultCollector;
import dk.statsbiblioteket.medieplatform.autonomous.iterator.bitrepository.IngesterConfiguration;
import org.bitrepository.bitrepositoryelements.ChecksumDataForFileTYPE;
import org.bitrepository.bitrepositoryelements.ChecksumSpecTYPE;
import org.bitrepository.client.eventhandler.CompleteEvent;
import org.bitrepository.client.eventhandler.EventHandler;
import org.bitrepository.common.settings.Settings;
import org.bitrepository.modify.putfile.PutFileClient;
import org.bitrepository.protocol.OperationType;
import org.testng.annotations.Test;

public class BitrepositoryIngesterComponentIT {
    private final static String TEST_BATCH_ID = "400022028241";
    /**
     * Tests that the ingester can parse a (small) production like batch.
     */
    @Test(groups = "integrationTest")
    public void smallBatchIngestCheck() throws Exception {
        String pathToConfig = System.getProperty("bitrepository.ingester.config");
        String pathToTestBatch = System.getProperty("integration.test.newspaper.testdata");
        Properties properties = new Properties();
        properties.load(new FileInputStream(pathToConfig + "/bitrepository-ingester.properties"));
        properties.setProperty(BitrepositoryIngesterComponent.SETTINGS_DIR_PROPERTY, pathToConfig);
        properties.setProperty("scratch", pathToTestBatch + "/" + "small-test-batch");

        BitrepositoryIngesterComponent bitrepositoryIngesterComponent =
                new StubbedBitrepositoryIngesterComponent(properties);

        ResultCollector resultCollector = new ResultCollector("Bitrepository ingester", "v0.1");
        Batch batch = new Batch();
        batch.setBatchID(TEST_BATCH_ID);
        batch.setRoundTripNumber(1);

        bitrepositoryIngesterComponent.doWorkOnBatch(batch, resultCollector);
    }
    /**
     * Tests that the ingester can parse a (small) production like batch.
     */
    //@Test(groups = "integrationTest")
    public void badBatchSurvivabilityCheck() throws Exception {
        String pathToConfig = System.getProperty("bitrepository.ingester.config");
        String pathToTestBatch = System.getProperty("integration.test.newspaper.testdata");
        Properties properties = new Properties();
        properties.load(new FileInputStream(pathToConfig + "/bitrepository-ingester.properties"));
        properties.setProperty(BitrepositoryIngesterComponent.SETTINGS_DIR_PROPERTY, pathToConfig);
        properties.setProperty("scratch", pathToTestBatch + "/" + "bad-bad-batch");

        BitrepositoryIngesterComponent bitrepositoryIngesterComponent =
                new StubbedBitrepositoryIngesterComponent(properties);

        ResultCollector resultCollector = new ResultCollector("Bitrepository ingester", "v0.1");
        Batch batch = new Batch();
        batch.setBatchID(TEST_BATCH_ID);
        batch.setRoundTripNumber(1);

        bitrepositoryIngesterComponent.doWorkOnBatch(batch, resultCollector);
    }

    private class StubbedBitrepositoryIngesterComponent extends BitrepositoryIngesterComponent {
        PutFileClientStub clientStub = new PutFileClientStub();
        Settings settings;

        public StubbedBitrepositoryIngesterComponent(Properties properties) {
            super(properties);
        }

        @Override
        protected PutFileClient createPutFileClient(IngesterConfiguration configuration, Settings settings) {
            return clientStub;
        }

        @Override
        protected Settings loadSettings(IngesterConfiguration configuration) {
            return super.loadSettings(configuration);
        }
    }

    private class PutFileClientStub implements PutFileClient {
        List<ActivePutOperation> runningOperations = new ArrayList<>();

        @Override
        public void putFile(String collectionID, URL url, String fileId, long sizeOfFile,
                            ChecksumDataForFileTYPE checksumForValidationAtPillar,
                            ChecksumSpecTYPE checksumRequestsForValidation, EventHandler eventHandler,
                            String auditTrailInformation) {
            runningOperations.add(new ActivePutOperation(collectionID, url, fileId, sizeOfFile,
                    checksumForValidationAtPillar, checksumRequestsForValidation, eventHandler, auditTrailInformation));
            CompleteEvent completeEvent = new CompleteEvent(null, null);
            completeEvent.setFileID(fileId);
            completeEvent.setOperationType(OperationType.PUT_FILE);
            eventHandler.handleEvent(completeEvent);
        }
    }

    class ActivePutOperation {
        String collectionID;
        URL url;
        String fileId;
        long sizeOfFile;
        ChecksumDataForFileTYPE checksumForValidationAtPillar;
        ChecksumSpecTYPE checksumRequestsForValidation;
        EventHandler eventHandler;
        String auditTrailInformation;

        ActivePutOperation(String collectionID, URL url, String fileId, long sizeOfFile, ChecksumDataForFileTYPE checksumForValidationAtPillar, ChecksumSpecTYPE checksumRequestsForValidation, EventHandler eventHandler, String auditTrailInformation) {
            this.collectionID = collectionID;
            this.url = url;
            this.fileId = fileId;
            this.sizeOfFile = sizeOfFile;
            this.checksumForValidationAtPillar = checksumForValidationAtPillar;
            this.checksumRequestsForValidation = checksumRequestsForValidation;
            this.eventHandler = eventHandler;
            this.auditTrailInformation = auditTrailInformation;
        }
    }
}
