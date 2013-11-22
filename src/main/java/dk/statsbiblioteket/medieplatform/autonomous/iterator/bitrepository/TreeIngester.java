package dk.statsbiblioteket.medieplatform.autonomous.iterator.bitrepository;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.jms.JMSException;

import dk.statsbiblioteket.medieplatform.autonomous.ResultCollector;
import dk.statsbiblioteket.newspaper.bitrepository.ingester.DomsJP2FileUrlRegister;
import dk.statsbiblioteket.newspaper.bitrepository.ingester.DomsObjectNotFoundException;
import dk.statsbiblioteket.util.Strings;

import org.bitrepository.client.eventhandler.EventHandler;
import org.bitrepository.client.eventhandler.OperationEvent;
import org.bitrepository.common.utils.Base16Utils;
import org.bitrepository.modify.putfile.PutFileClient;
import org.bitrepository.protocol.messagebus.MessageBus;
import org.bitrepository.protocol.messagebus.MessageBusManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class handling ingest of a set of files in a tree iterator structure.
 */
public class TreeIngester {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private static final long DEFAULT_FILE_SIZE = 0;

    private final ResultCollector resultCollector;
    private final IngestableFileLocator fileLocator;
    private final String collectionID;
    private final OperationEventHandler handler;
    private final ParallelOperationLimiter parallelOperationLimiter;
    private final PutFileClient putFileClient;
    private final DomsJP2FileUrlRegister urlRegister;
    private final String baseUrl;

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
    public TreeIngester(
            String collectionID,
            long timoutForLastOperation,
            IngestableFileLocator fileLocator,
            PutFileClient putFileClient,
            ResultCollector resultCollector,
            int maxNumberOfParallelPuts,
            DomsJP2FileUrlRegister urlRegister,
            String baseUrl) {
        this.collectionID = collectionID;
        this.resultCollector = resultCollector;
        this.fileLocator = fileLocator;
        parallelOperationLimiter = new ParallelOperationLimiter(
                maxNumberOfParallelPuts, (int)timoutForLastOperation/1000);
        handler = new OperationEventHandler(parallelOperationLimiter);
        this.putFileClient = putFileClient;
        this.urlRegister = urlRegister;
        this.baseUrl = baseUrl;
    }

    public void performIngest() {
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

    protected class OperationEventHandler implements EventHandler {
        private final ParallelOperationLimiter operationLimiter;
        public OperationEventHandler(ParallelOperationLimiter putLimiter) {
            this.operationLimiter = putLimiter;
        }

        @Override
        public void handleEvent(OperationEvent event) {
            if (event.getEventType().equals(OperationEvent.OperationEventType.COMPLETE)) {
                PutJob job = getJob(event);
                log.debug("Completed ingest of file " + event.getFileID());
                String url = baseUrl + job.getFileID();
                try {
                    urlRegister.registerJp2File(job.getPath(), job.getFileID(), url, job.getChecksum());
                } catch (DomsObjectNotFoundException e) {
                    resultCollector.addFailure(event.getFileID(), "ingest", getClass().getSimpleName(),
                            "Could not find the proper DOMS object to register the ingested file to: " + e.toString(),
                            Strings.getStackTrace(e));
                }
                operationLimiter.removeJob(job);
                
            } else if (event.getEventType().equals(OperationEvent.OperationEventType.FAILED)) {
                PutJob job = getJob(event);
                log.warn("Failed to ingest file " + event.getFileID() + ", Cause: " + event);
                resultCollector.addFailure(event.getFileID(), "ingest", getClass().getSimpleName(), event.getInfo());
                operationLimiter.removeJob(job);
            }
        }

        private PutJob getJob(OperationEvent event) {
            PutJob job = null;
            job = operationLimiter.getJob(event.getFileID());
            return job;
        }
    }
    
    
    private class PutJob {
        private final String fileID;
        private final String checksum;
        private final String path;
        
        PutJob(String fileID, String checksum, String path) {
            this.fileID = fileID;
            this.checksum = checksum;
            this.path = path;
        }
        
        String getFileID() {
            return fileID;
        }
        
        String getChecksum() {
            return checksum;
        }
        
        String getPath() {
            return path;
        }
    }

    /**
     * Provides functionality for limiting the number of operations by providing a addJob method which
     * will block if a specified limit is reached.
     */
    protected class ParallelOperationLimiter {
        private final BlockingQueue<PutJob> activeOperations;
        private final int secondsToWaitForFinish;

        ParallelOperationLimiter(int limit, int timeToWaitForFinish) {
            activeOperations = new LinkedBlockingQueue<>(limit);
            this.secondsToWaitForFinish = timeToWaitForFinish;
        }

        /**
         * Will block until the if the activeOperations queue limit is exceeded and unblock when a job is removed.
         * @param job The job in the queue.
         */
        void addJob(PutJob job) {
            try {
                activeOperations.put(job);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        
        /**
         * Gets the PutJob for fileID
         * @param fileID The fileID to get the job for
         * @return PutJob the PutJob with relevant info for the job. 
         */
        PutJob getJob(String fileID) {
            Iterator<PutJob> iter = activeOperations.iterator();
            PutJob job = null;
            while(iter.hasNext()) {
                job = iter.next();
                if(job.getFileID().equals(fileID)) {
                    break;
                }
            }
            return job;
        }

        /**
         * Removes a job from the queue
         * @param job the PutJob to remove 
         */
        void removeJob(PutJob job) {
            activeOperations.remove(job);
        }

        public void waitForFinish() {
            int secondsWaiting = 0;
            while (!activeOperations.isEmpty() && (secondsWaiting++ < secondsToWaitForFinish)) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    //No problem
                }
            }
            if (secondsWaiting > secondsToWaitForFinish) {
                String message = "Timeout(" + secondsToWaitForFinish+ "s) waiting for last files (" + Arrays.toString(activeOperations.toArray()) + ")to complete.";
                log.warn(message);
                for (PutJob job : activeOperations.toArray(new PutJob[activeOperations.size()])) {
                    resultCollector.addFailure(job.getFileID(), "ingest", getClass().getSimpleName(),
                            "Timeout waiting for last files to be ingested.");
                }
            }
        }
    }
}
