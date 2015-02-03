package dk.statsbiblioteket.medieplatform.autonomous.iterator.bitrepository;

import javax.jms.JMSException;

import org.bitrepository.client.eventhandler.EventHandler;
import org.bitrepository.common.utils.Base16Utils;
import org.bitrepository.modify.putfile.PutFileClient;
import org.bitrepository.protocol.messagebus.MessageBus;
import org.bitrepository.protocol.messagebus.MessageBusManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dk.statsbiblioteket.medieplatform.autonomous.ResultCollector;
import dk.statsbiblioteket.newspaper.bitrepository.ingester.DomsJP2FileUrlRegister;

/**
 * Class handling ingest of a set of files in a tree iterator structure.
 */
public class TreeIngester {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private static final long DEFAULT_FILE_SIZE = 0;
    private final IngestableFileLocator fileLocator;
    private final String collectionID;
    private final EventHandler handler;
    private final ParallelOperationLimiter parallelOperationLimiter;
    private final PutFileClient putFileClient;

    /**
     *
     * @param collectionID The collectionID of the collection to store the ingested files in.
     * @param timoutForLastOperation How many milliseconds should the ingester wait for the last files found in the tree
     *                               to finish ingesting before force quitting .
     * @param fileLocator Used for finding the relevant files.
     * @param putFileClient For handling the actual ingests.
     * @param resultCollector Failures are logged here.
     * @param maxNumberOfParallelPuts The number of puts to to perform in parallel.
     */
    public TreeIngester(String collectionID, ParallelOperationLimiter operationLimiter, DomsJP2FileUrlRegister domsRegistor, 
            IngestableFileLocator fileLocator, PutFileClient putFileClient, ResultCollector resultCollector) {
        this.collectionID = collectionID;
        this.fileLocator = fileLocator;
        this.parallelOperationLimiter = operationLimiter;
        handler = new PutFileEventHandler(parallelOperationLimiter, domsRegistor, resultCollector);
        this.putFileClient = putFileClient;
    }

    public void performIngest() throws NotFinishedException {
        IngestableFile file = null;
        do {
            try {
                file = fileLocator.nextFile();
                try {
                    if (file != null) {
                        putFile(file);
                    }
                } catch (Exception e) {
                    log.error("Failed to ingest file.", e);
                }
            } catch (Exception e) {
                log.error("Failed to find file to ingest.", e);
            }
        }  while (file != null);

        parallelOperationLimiter.waitForFinish();

    }

    /**
     * Calls the concrete putFileClient blocking if the maxNumberOfParallelPut are exceeded.
     */
    private void putFile(IngestableFile ingestableFile) {
        PutJob job = new PutJob(ingestableFile.getFileID(), 
                Base16Utils.decodeBase16(ingestableFile.getChecksum().getChecksumValue()),
                ingestableFile.getPath());
        parallelOperationLimiter.addJob(job);
        putFileClient.putFile(collectionID,
                ingestableFile.getUrl(), ingestableFile.getFileID(), DEFAULT_FILE_SIZE,
                ingestableFile.getChecksum(), null, handler, null);
    }

    /**
     * Method to shutdown the client properly.
     */
    public void shutdown() {
        try {
            MessageBus messageBus = MessageBusManager.getMessageBus();
            if (messageBus != null) {
                MessageBusManager.getMessageBus().close();
            }
        } catch (JMSException e) {
            log.warn("Failed to shutdown messagebus connection", e);
        } catch (Exception e) {
            log.warn("Caught unexpected exception while closing messagebus down", e);
        }
    }
}
