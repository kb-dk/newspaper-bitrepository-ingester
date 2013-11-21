package dk.statsbiblioteket.newspaper.bitrepository.ingester;

import java.util.List;

import dk.statsbiblioteket.doms.central.connectors.BackendInvalidCredsException;
import dk.statsbiblioteket.doms.central.connectors.BackendInvalidResourceException;
import dk.statsbiblioteket.doms.central.connectors.BackendMethodFailedException;
import dk.statsbiblioteket.doms.central.connectors.EnhancedFedora;

/**
 * Class to handle the registration of the bit repository URL for a given JP2000 file in DOMS.  
 */
public class DomsJP2FileUrlRegister {

    public static final String JP2_MIMETYPE = "image/jp2";
    public static final String PATH_PREFIX = "path:";
    public static final String RELATION_PREDICATE = "http://doms.statsbiblioteket.dk/relations/default/0/1/#hasMD5";
    public static final String CONTENTS = "contents";
    
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
     */
    public void registerJp2File(String path, String filename, String url, String checksum) throws DomsObjectNotFoundException {
        List<String> objects;
        try {
            objects = enhancedFedora.findObjectFromDCIdentifier(PATH_PREFIX + path);

            if(objects.size() != 1) {
                throw new DomsObjectNotFoundException("Expected excatly 1 identifier from DOMS, got " + objects.size()
                        + ". Don't know where to add file.");
            }
            String fileObjectPid = objects.get(0);
            enhancedFedora.addExternalDatastream(fileObjectPid, CONTENTS, filename, url, null, 
                    JP2_MIMETYPE, null, "Adding file after bitrepository ingest");
            enhancedFedora.addRelation(fileObjectPid, "info:fedora/" + fileObjectPid + "/" + CONTENTS, RELATION_PREDICATE,
                    checksum, true, "Adding file after bitrepository ingest");
        } catch (BackendInvalidCredsException | BackendMethodFailedException | BackendInvalidResourceException e) {
            throw new RuntimeException(e.getMessage(), e); 
        }
        
    }

}
