package dk.statsbiblioteket.newspaper.bitrepository.ingester;

import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dk.statsbiblioteket.doms.central.connectors.BackendInvalidCredsException;
import dk.statsbiblioteket.doms.central.connectors.BackendInvalidResourceException;
import dk.statsbiblioteket.doms.central.connectors.BackendMethodFailedException;
import dk.statsbiblioteket.doms.central.connectors.EnhancedFedora;

/**
 * Handle the registration of the bit repository URL for a given JP2000 file in DOMS.  
 */
public class DomsJP2FileUrlRegister {
    private final Logger log = LoggerFactory.getLogger(getClass());
    public static final String JP2_MIMETYPE = "image/jp2";
    public static final String RELATION_PREDICATE = "http://doms.statsbiblioteket.dk/relations/default/0/1/#hasMD5";
    public static final String CONTENTS = "CONTENTS";
    
    private EnhancedFedora enhancedFedora;
    
    public DomsJP2FileUrlRegister(EnhancedFedora central) {
        this.enhancedFedora = central;
    }
    
    /**
     * Register the location of a file in the doms object identified by path. 
     * @param path The path identifying the DOMS object to register the file content to
     * @param filename The name of the file to be registered to the DOMS object
     * @param url The url to where the file content can be resolved. 
     * @param checksum The checksum of the data 
     * @throws DomsObjectNotFoundException when there's either too many objects found or non at all.
     * @throws BackendMethodFailedException 
     * @throws BackendInvalidCredsException 
     * @throws BackendInvalidResourceException 
     */
    public void registerJp2File(String path, String filename, String url, String checksum) throws DomsObjectNotFoundException, BackendInvalidCredsException, BackendMethodFailedException, BackendInvalidResourceException {
        List<String> objects;
        Date start = new Date();
        objects = enhancedFedora.findObjectFromDCIdentifier(path);
        Date objFound = new Date();
        log.trace("It took {} ms to find object in doms for path '{}'", objFound.getTime() - start.getTime(), path);
        if(objects.size() != 1) {
            throw new DomsObjectNotFoundException("Expected excatly 1 identifier from DOMS, got " + objects.size()
                    + " for object with DCIdentifier: '" + path + "'. Don't know where to add file.");
        }
        String fileObjectPid = objects.get(0);
        enhancedFedora.addExternalDatastream(fileObjectPid, CONTENTS, filename, url, "application/octet-stream", 
                JP2_MIMETYPE, null, "Adding file after bitrepository ingest");
        Date dsAdded = new Date();
        log.trace("It took {} ms to add external datastream to doms for path '{}'", dsAdded.getTime() - objFound.getTime(), path);
        enhancedFedora.addRelation(fileObjectPid, "info:fedora/" + fileObjectPid + "/" + CONTENTS, RELATION_PREDICATE,
                checksum, true, "Adding file after bitrepository ingest");
        Date finished = new Date();
        log.trace("It took {} ms to add relation in doms for path '{}'", finished.getTime() - dsAdded.getTime(), path);
        log.trace("In total it took {} ms to register file in doms for path '{}'", finished.getTime() - start.getTime(), path);
    }

}