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
     */
    public void registerJp2File(String path, String filename, String url) {
        List<String> objects;
        try {
            objects = enhancedFedora.findObjectFromDCIdentifier(PATH_PREFIX + path);

            if(objects.size() != 1) {
                throw new RuntimeException("Expected excatly 1 identifier from DOMS, got " + objects.size()
                        + ". Don't know where to add file.");
            }
            String fileObjectPid = objects.get(0);
            enhancedFedora.addExternalDatastream(fileObjectPid, "contents", filename, url, null, 
                    JP2_MIMETYPE, null, "Adding file after bitrepository ingest");
        } catch (BackendInvalidCredsException | BackendMethodFailedException | BackendInvalidResourceException e) {
            throw new RuntimeException(e.getMessage(), e); 
        }
        
    }

}
