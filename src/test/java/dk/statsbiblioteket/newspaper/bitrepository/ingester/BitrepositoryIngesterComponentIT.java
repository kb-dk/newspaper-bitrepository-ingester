package dk.statsbiblioteket.newspaper.bitrepository.ingester;

import java.io.FileInputStream;
import java.util.Properties;

import dk.statsbiblioteket.medieplatform.autonomous.Batch;
import dk.statsbiblioteket.medieplatform.autonomous.ResultCollector;
import dk.statsbiblioteket.medieplatform.autonomous.iterator.bitrepository.IngesterConfiguration;
import org.bitrepository.common.settings.Settings;
import org.bitrepository.modify.putfile.PutFileClient;
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
        properties.load(new FileInputStream(pathToConfig + "/config.properties"));
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
    @Test(groups = "integrationTest")
    public void badBatchSurvivabilityCheck() throws Exception {
        String pathToConfig = System.getProperty("bitrepository.ingester.config");
        String pathToTestBatch = System.getProperty("integration.test.newspaper.testdata");
        Properties properties = new Properties();
        properties.load(new FileInputStream(pathToConfig + "/config.properties"));
        properties.setProperty(BitrepositoryIngesterComponent.SETTINGS_DIR_PROPERTY, pathToConfig);
        properties.setProperty("scratch", pathToTestBatch + "/" + "bad-bad-batch");

        BitrepositoryIngesterComponent bitrepositoryIngesterComponent =
                new StubbedBitrepositoryIngesterComponent(properties);

        ResultCollector resultCollector = new ResultCollector("Mocked bitrepository ingester", "test version");
        Batch batch = new Batch();
        batch.setBatchID(TEST_BATCH_ID);
        batch.setRoundTripNumber(1);

        bitrepositoryIngesterComponent.doWorkOnBatch(batch, resultCollector);
    }

    private class StubbedBitrepositoryIngesterComponent extends BitrepositoryIngesterComponent {
        PutFileClientStub clientStub = new PutFileClientStub();

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
}
