package dk.statsbiblioteket.newspaper.bitrepository.ingester;

import java.util.List;

import dk.statsbiblioteket.doms.central.CentralWebservice;
import dk.statsbiblioteket.doms.central.InvalidCredentialsException;
import dk.statsbiblioteket.doms.central.InvalidResourceException;
import dk.statsbiblioteket.doms.central.MethodFailedException;

/**
 * Class to handle the registration of the bit repository URL for a given JP2000 file in DOMS.  
 */
public class DomsJP2FileUrlRegister {

    public static final String JP2_FORMAT_URI = "image/jp2";
    public static final String PATH_PREFIX = "path:";
    
    private CentralWebservice central;
    
    public DomsJP2FileUrlRegister(CentralWebservice central) {
        this.central = central;
    }
    
    /**
     * Register the location of a file in the doms object identified by path. 
     * @param path The path identifying the DOMS object to register the file content to
     * @param filename The name of the file to be registered to the DOMS object
     * @param url The url to where the file content can be resolved. 
     * @param checksum The checksum of the data 
     */
    public void registerJp2File(String path, String filename, String url) {
        try {
            List<String> objects = central.findObjectFromDCIdentifier(PATH_PREFIX + path);
            if(objects.size() > 1) {
                throw new RuntimeException("Got multiple identifiers from DOMS, "
                        + "don't know which to add file to.");
            }
            String fileObjectPid = objects.get(0);
            central.addFileFromPermanentURL(fileObjectPid, filename, null, 
                    url, JP2_FORMAT_URI, "Adding file after bitrepository ingest");
            
        } catch (InvalidCredentialsException | MethodFailedException | InvalidResourceException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

}
