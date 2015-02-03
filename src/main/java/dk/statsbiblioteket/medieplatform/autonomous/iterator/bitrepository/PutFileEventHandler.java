package dk.statsbiblioteket.medieplatform.autonomous.iterator.bitrepository;

import java.util.ArrayList;
import java.util.List;

import org.bitrepository.client.eventhandler.ContributorEvent;
import org.bitrepository.client.eventhandler.EventHandler;
import org.bitrepository.client.eventhandler.OperationEvent;
import org.bitrepository.client.eventhandler.OperationFailedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dk.statsbiblioteket.medieplatform.autonomous.ResultCollector;
import dk.statsbiblioteket.newspaper.bitrepository.ingester.DomsJP2FileUrlRegister;

public class PutFileEventHandler implements EventHandler {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ParallelOperationLimiter operationLimiter;
    private final DomsJP2FileUrlRegister domsRegistor;
    private final ResultCollector resultCollector;
    
    public PutFileEventHandler(ParallelOperationLimiter putLimiter, DomsJP2FileUrlRegister domsRegistor, ResultCollector resultCollector) {
    	this.operationLimiter = putLimiter;
        this.domsRegistor = domsRegistor;
        this.resultCollector = resultCollector;
    }

    @Override
    public void handleEvent(OperationEvent event) {
        if (event.getEventType().equals(OperationEvent.OperationEventType.COMPLETE)) {
            PutJob job = getJob(event);
            log.debug("Completed ingest of file " + event.getFileID());
            domsRegistor.registerJp2File(job);
            operationLimiter.removeJob(job);
        } else if (event.getEventType().equals(OperationEvent.OperationEventType.FAILED)) {
            PutJob job = getJob(event);
            log.warn("Failed to ingest file " + event.getFileID() + ", Cause: " + event);
            List<String> components = new ArrayList<String>();
            if(event instanceof OperationFailedEvent) {
                OperationFailedEvent opEvent = (OperationFailedEvent) event;
                List<ContributorEvent> events = opEvent.getComponentResults();
                for(ContributorEvent e : events) {
                    if(e.getEventType().equals(OperationEvent.OperationEventType.COMPONENT_FAILED)) {
                        components.add(e.getContributorID());
                    }
                }
            }
            String failureDetails = "Failed conversation '" + event.getConversationID() 
                    + "' with reason: '" + event.getInfo() + "' for components: " +components;
            resultCollector.addFailure(event.getFileID(), "jp2file", getClass().getSimpleName(), failureDetails);
            operationLimiter.removeJob(job);
        }
    }

    private PutJob getJob(OperationEvent event) {
        PutJob job = null;
        job = operationLimiter.getJob(event.getFileID());
        return job;
    }
}
