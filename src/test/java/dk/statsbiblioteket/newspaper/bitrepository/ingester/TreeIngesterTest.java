package dk.statsbiblioteket.newspaper.bitrepository.ingester;

import dk.statsbiblioteket.medieplatform.autonomous.ResultCollector;
import dk.statsbiblioteket.medieplatform.autonomous.iterator.bitrepository.TreeIngester;
import org.bitrepository.modify.putfile.PutFileClient;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;

public class TreeIngesterTest {
    public final static String TEST_COLLECTION_ID = "testCollection";
    public static final String BATCH_DIR_URL = "file://batchDirUrl";
    protected static String DEFAULT_MD5_CHECKSUM = "1234cccccccc4321";
    private BatchImageLocator fileLocator;
    private PutFileClient putFileClient;
    private ResultCollector resultCollector;
    private TreeIngester treeIngester;

    @BeforeMethod
    public void setupTreeIngester() {
        fileLocator = mock(BatchImageLocator.class);
        putFileClient = new PutFileClientStub();
        resultCollector = mock(ResultCollector.class);
        treeIngester = new TreeIngester(
                TEST_COLLECTION_ID, fileLocator, putFileClient, resultCollector);
    }

    @Test
    public void emptyTreeTest() {
        treeIngester.performIngest();
    }

    @Test
    public void singleNodeTest() {
        //Todo
        treeIngester.performIngest();
    }
}
