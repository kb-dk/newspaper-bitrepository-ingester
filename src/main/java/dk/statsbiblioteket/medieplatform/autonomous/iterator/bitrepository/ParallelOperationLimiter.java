package dk.statsbiblioteket.medieplatform.autonomous.iterator.bitrepository;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for holding jobs 
 */
public class ParallelOperationLimiter {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final BlockingQueue<PutJob> activeOperations;
    private final int secondsToWaitForFinish;

    public ParallelOperationLimiter(int limit, int timeToWaitForFinish) {
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
     * @return PutJob the PutJob with relevant info for the job. May return null if no job matching fileID is found
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
    
    PutJob getNextJob() throws InterruptedException {
    	return activeOperations.take();
    }

    /**
     * Removes a job from the queue
     * @param job the PutJob to remove 
     */
    void removeJob(PutJob job) {
        activeOperations.remove(job);
    }

    public void waitForFinish() throws NotFinishedException {
        int secondsWaiting = 0;
        while (!activeOperations.isEmpty() && (secondsWaiting++ < secondsToWaitForFinish)) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                //No problem
            }
        }
        // There's a theroretical chance/possiblity that activeOperations still may change here (both grow and shrink)
        if (secondsWaiting > secondsToWaitForFinish) {
            throw new NotFinishedException(activeOperations);
        }
    }
}
