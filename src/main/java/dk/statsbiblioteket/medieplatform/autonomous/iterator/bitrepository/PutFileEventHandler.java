package dk.statsbiblioteket.medieplatform.autonomous.iterator.bitrepository;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.bitrepository.client.eventhandler.ContributorEvent;
import org.bitrepository.client.eventhandler.EventHandler;
import org.bitrepository.client.eventhandler.OperationEvent;
import org.bitrepository.client.eventhandler.OperationFailedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dk.statsbiblioteket.newspaper.bitrepository.ingester.DomsJP2FileUrlRegister;

public class PutFileEventHandler implements EventHandler {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ParallelOperationLimiter operationLimiter;
    private final DomsJP2FileUrlRegister domsRegister;
    private final BlockingQueue<PutJob> failedJobs;
    
    public PutFileEventHandler(ParallelOperationLimiter putLimiter, BlockingQueue<PutJob> failedJobsQueue, 
            DomsJP2FileUrlRegister domsRegister) {
    	this.operationLimiter = putLimiter;
        this.domsRegister = domsRegister;
        this.failedJobs = failedJobsQueue;
    }

    @Override
    public void handleEvent(OperationEvent event) {
        if (event.getEventType().equals(OperationEvent.OperationEventType.COMPLETE)) {
            PutJob job = getJob(event);
            if(job != null) {
                log.debug("Completed ingest of file " + event.getFileID());
                domsRegister.registerJp2File(job);
                operationLimiter.removeJob(job);
            }
        } else if (event.getEventType().equals(OperationEvent.OperationEventType.FAILED)) {
            PutJob job = getJob(event);
            if(job != null) {
                log.warn("Failed to ingest file " + event.getFileID() + ", Cause: " + event);
                List<String> components = new ArrayList<String>();
                if(event instanceof OperationFailedEvent) {
                    OperationFailedEvent opEvent = (OperationFailedEvent) event;
                    List<ContributorEvent> events = opEvent.getComponentResults();
                    if(events != null) {
                        for(ContributorEvent e : events) {
                            if(e.getEventType().equals(OperationEvent.OperationEventType.COMPONENT_FAILED)) {
                                components.add(e.getContributorID());
                            }
                        }
                    }
                }
                String failureDetails = "Failed conversation '" + event.getConversationID() 
                        + "' with reason: '" + event.getInfo() + "' for components: " +components;
                job.addResultMessage(failureDetails);
                failedJobs.add(job);
                operationLimiter.removeJob(job);
            }
        }
    }

    private PutJob getJob(OperationEvent event) {
        PutJob job = null;
        job = operationLimiter.getJob(event.getFileID());
        return job;
    }
}
