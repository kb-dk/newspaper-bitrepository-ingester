package dk.statsbiblioteket.newspaper.bitrepository.ingester;

import java.net.MalformedURLException;
import java.net.URL;

import dk.statsbiblioteket.medieplatform.autonomous.ResultCollector;
import dk.statsbiblioteket.medieplatform.autonomous.iterator.bitrepository.IngestableFile;
import dk.statsbiblioteket.medieplatform.autonomous.iterator.bitrepository.TreeIngester;
import org.bitrepository.bitrepositoryelements.ChecksumDataForFileTYPE;
import org.bitrepository.bitrepositoryelements.ChecksumSpecTYPE;
import org.bitrepository.bitrepositoryelements.ChecksumType;
import org.bitrepository.client.eventhandler.CompleteEvent;
import org.bitrepository.client.eventhandler.EventHandler;
import org.bitrepository.common.utils.Base16Utils;
import org.bitrepository.common.utils.CalendarUtils;
import org.bitrepository.modify.putfile.PutFileClient;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class TreeIngesterTest {
    public final static String TEST_COLLECTION_ID = "testCollection";
    protected static final int DEFAULT_MAX_NUMBER_OF_PARALLEL_PUTS = 10;
    private BatchImageLocator fileLocator;
    private PutFileClient putFileClient;
    private ResultCollector resultCollector;
    private TreeIngester treeIngester;

    @BeforeMethod
    public void setupTreeIngester() {
        fileLocator = mock(BatchImageLocator.class);
        putFileClient = new PutFileClientStub();
        resultCollector = mock(ResultCollector.class);
    }

    @Test
    public void emptyTreeTest() {
        treeIngester = new TreeIngester(
                TEST_COLLECTION_ID, fileLocator, putFileClient, resultCollector, DEFAULT_MAX_NUMBER_OF_PARALLEL_PUTS);
        treeIngester.performIngest();
    }

    @Test
    public void parallelPutTest() throws MalformedURLException, InterruptedException {
        putFileClient = mock(PutFileClient.class);
        int maxNumberOfParallelPuts = 2;
        ChecksumDataForFileTYPE checksum = getChecksum("aa");
        treeIngester = new TreeIngester(
                TEST_COLLECTION_ID, fileLocator, putFileClient, resultCollector, maxNumberOfParallelPuts);
        IngestableFile firstFile =
                new IngestableFile("First-file", new URL("http://somewhere.someplace/first-file"), checksum, 0L);
        IngestableFile secondFile =
                new IngestableFile("Second-file", new URL("http://somewhere.someplace/second-file"), checksum, 0L);
        IngestableFile thirdFile =
                new IngestableFile("Third-file", new URL("http://somewhere.someplace/third-file"), checksum, 0L);
        IngestableFile fourthFile =
                new IngestableFile("Fourth-file", new URL("http://somewhere.someplace/fourthFile-file"), checksum, 0L);
        when(fileLocator.nextFile()).thenReturn(firstFile).thenReturn(secondFile).thenReturn(thirdFile).thenReturn(fourthFile);

        //We need to run the ingest in a separate thread, as it will block.
        Thread t = new Thread(new Runnable() {
            public void run() {
                treeIngester.performIngest();
            }
        });
        t.start();
        Thread.sleep(100);

        ArgumentCaptor<EventHandler> eventHandlerCaptor = ArgumentCaptor.forClass(EventHandler.class);
        verify(putFileClient).putFile(
                eq(TEST_COLLECTION_ID), eq(firstFile.getUrl()), eq(firstFile.getFileID()), eq(0L),
                eq(checksum), (ChecksumSpecTYPE) isNull(), eventHandlerCaptor.capture(), (String) isNull());
        verify(putFileClient).putFile(
                eq(TEST_COLLECTION_ID), eq(secondFile.getUrl()), eq(secondFile.getFileID()), eq(0L),
                eq(checksum), (ChecksumSpecTYPE) isNull(), (EventHandler)anyObject(), (String) isNull());
        verifyNoMoreInteractions(putFileClient);

        CompleteEvent firstFileComplete = new CompleteEvent(TEST_COLLECTION_ID, null);
        firstFileComplete.setFileID(firstFile.getFileID());
        eventHandlerCaptor.getValue().handleEvent(firstFileComplete);
        Thread.sleep(100);
        verify(putFileClient).putFile(
                eq(TEST_COLLECTION_ID), eq(thirdFile.getUrl()), eq(thirdFile.getFileID()), eq(0L),
                eq(checksum), (ChecksumSpecTYPE) isNull(), (EventHandler)anyObject(), (String) isNull());

        CompleteEvent secondFileComplete = new CompleteEvent(TEST_COLLECTION_ID, null);
        secondFileComplete.setFileID(secondFile.getFileID());
        eventHandlerCaptor.getValue().handleEvent(secondFileComplete);
        Thread.sleep(100);
        verify(putFileClient).putFile(
                eq(TEST_COLLECTION_ID), eq(fourthFile.getUrl()), eq(fourthFile.getFileID()), eq(0L),
                eq(checksum), (ChecksumSpecTYPE) isNull(), (EventHandler)anyObject(), (String) isNull());
    }

    private ChecksumDataForFileTYPE getChecksum(String checksum) {
        ChecksumDataForFileTYPE checksumData = new ChecksumDataForFileTYPE();
        checksumData.setChecksumValue(Base16Utils.encodeBase16(checksum));
        checksumData.setCalculationTimestamp(CalendarUtils.getNow());
        ChecksumSpecTYPE checksumSpec = new ChecksumSpecTYPE();
        checksumSpec.setChecksumType(ChecksumType.MD5);
        checksumData.setChecksumSpec(checksumSpec);
        return checksumData;
    }
}
