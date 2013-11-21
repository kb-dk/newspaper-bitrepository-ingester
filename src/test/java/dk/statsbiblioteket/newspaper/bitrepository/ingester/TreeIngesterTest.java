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
import static org.testng.Assert.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

public class TreeIngesterTest {
    public final static String TEST_COLLECTION_ID = "testCollection";
    protected static final int DEFAULT_MAX_NUMBER_OF_PARALLEL_PUTS = 10;
    protected static final String DEFAULT_BASEURL = "http://bitfinder.statsbiblioteket.dk/avis/";
    private BatchImageLocator fileLocator;
    private PutFileClient putFileClient;
    private ResultCollector resultCollector;
    private TreeIngester treeIngester;
    private DomsJP2FileUrlRegister urlRegister;

    @BeforeMethod
    public void setupTreeIngester() {
        fileLocator = mock(BatchImageLocator.class);
        putFileClient = new PutFileClientStub();
        resultCollector = mock(ResultCollector.class);
        urlRegister = mock(DomsJP2FileUrlRegister.class);
    }

    @Test
    public void emptyTreeTest() {
        treeIngester = new TreeIngester(
                TEST_COLLECTION_ID, 0, fileLocator, putFileClient, resultCollector, DEFAULT_MAX_NUMBER_OF_PARALLEL_PUTS,
                urlRegister, DEFAULT_BASEURL);
        treeIngester.performIngest();
    }

    @Test
    public void parallelPutTest() throws MalformedURLException, InterruptedException {
        putFileClient = mock(PutFileClient.class);
        int maxNumberOfParallelPuts = 2;
        ChecksumDataForFileTYPE checksum = getChecksum("aa");
        treeIngester = new TreeIngester(
                TEST_COLLECTION_ID, 0, fileLocator, putFileClient, resultCollector, maxNumberOfParallelPuts,
                urlRegister, DEFAULT_BASEURL);
        IngestableFile firstFile =
                new IngestableFile("First-file", new URL("http://somewhere.someplace/first-file"), checksum, 0L,
                        "path:First-file");
        IngestableFile secondFile =
                new IngestableFile("Second-file", new URL("http://somewhere.someplace/second-file"), checksum, 0L,
                        "path:Second-file");
        IngestableFile thirdFile =
                new IngestableFile("Third-file", new URL("http://somewhere.someplace/third-file"), checksum, 0L,
                        "path:Third-file");
        IngestableFile fourthFile =
                new IngestableFile("Fourth-file", new URL("http://somewhere.someplace/fourthFile-file"), checksum, 0L,
                        "path:Fourth-file");
        when(fileLocator.nextFile()).thenReturn(firstFile).thenReturn(secondFile).thenReturn(thirdFile).thenReturn(fourthFile);

        //We need to run the ingest in a separate thread, as it will block.
        Thread t = new Thread(new TreeIngestRunner());
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

    /**
     * Tests that the ingester correctly waits for the last put to complete before exiting.
     */
    @Test
    public void parallelPutCompletionTest() throws MalformedURLException, InterruptedException {
        final int TIMEOUT_FOR_OPERATION = 10000;
        putFileClient = mock(PutFileClient.class);
        int maxNumberOfParallelPuts = 1;
        ChecksumDataForFileTYPE checksum = getChecksum("aa");
        treeIngester = new TreeIngester(
                TEST_COLLECTION_ID, TIMEOUT_FOR_OPERATION, fileLocator, putFileClient, resultCollector, 
                maxNumberOfParallelPuts, urlRegister, DEFAULT_BASEURL);
        IngestableFile firstFile =
                new IngestableFile("First-file", new URL("http://somewhere.someplace/first-file"), checksum, 0L,
                        "path:First-file");
        when(fileLocator.nextFile()).thenReturn(firstFile).thenReturn(null);

        //We need to run the ingest in a separate thread, as it will block.
        TreeIngestRunner runner = new TreeIngestRunner();
        Thread t = new Thread(runner);
        t.start();
        Thread.sleep(100);

        ArgumentCaptor<EventHandler> eventHandlerCaptor = ArgumentCaptor.forClass(EventHandler.class);
        verify(putFileClient).putFile(
                eq(TEST_COLLECTION_ID), eq(firstFile.getUrl()), eq(firstFile.getFileID()), eq(0L),
                eq(checksum), (ChecksumSpecTYPE) isNull(), eventHandlerCaptor.capture(), (String) isNull());
        Thread.sleep(1000);
        assertFalse(runner.finished);

        CompleteEvent firstFileComplete = new CompleteEvent(TEST_COLLECTION_ID, null);
        firstFileComplete.setFileID(firstFile.getFileID());
        eventHandlerCaptor.getValue().handleEvent(firstFileComplete);
        Thread.sleep(2000);
        assertTrue(runner.finished);
    }

    /**
     * Tests that the ingester correctly exists when the timeout expires.
     */
    @Test
    public void parallelPutTimeoutTest() throws MalformedURLException, InterruptedException {
        final int TIMEOUT_FOR_OPERATION = 1000;
        putFileClient = mock(PutFileClient.class);
        int maxNumberOfParallelPuts = 1;
        ChecksumDataForFileTYPE checksum = getChecksum("aa");
        treeIngester = new TreeIngester(
                TEST_COLLECTION_ID, TIMEOUT_FOR_OPERATION, fileLocator, putFileClient, resultCollector, 
                maxNumberOfParallelPuts, urlRegister, DEFAULT_BASEURL);
        IngestableFile firstFile =
                new IngestableFile("First-file", new URL("http://somewhere.someplace/first-file"), checksum, 0L,
                        "path:First-file");
        when(fileLocator.nextFile()).thenReturn(firstFile).thenReturn(null);

        //We need to run the ingest in a separate thread, as it will block.
        TreeIngestRunner runner = new TreeIngestRunner();
        Thread t = new Thread(runner);
        t.start();
        Thread.sleep(100);

        ArgumentCaptor<EventHandler> eventHandlerCaptor = ArgumentCaptor.forClass(EventHandler.class);
        verify(putFileClient).putFile(
                eq(TEST_COLLECTION_ID), eq(firstFile.getUrl()), eq(firstFile.getFileID()), eq(0L),
                eq(checksum), (ChecksumSpecTYPE) isNull(), eventHandlerCaptor.capture(), (String) isNull());
        Thread.sleep(500);
        assertFalse(runner.finished);
        Thread.sleep(2000);
        assertTrue(runner.finished);
        verify(resultCollector).addFailure(anyString(), anyString(), anyString(), anyString());
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

    private class TreeIngestRunner implements Runnable {
        boolean finished = false;

        public void run() {
            treeIngester.performIngest();
            finished = true;
        }
    }
}
