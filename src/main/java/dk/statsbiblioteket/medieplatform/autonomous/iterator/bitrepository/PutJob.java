package dk.statsbiblioteket.medieplatform.autonomous.iterator.bitrepository;

/**
 * Class to carry the information relevant to a put job 
 */
public class PutJob {
    private final String fileID;
    private final String checksum;
    private final String path;
    
    public PutJob(String fileID, String checksum, String path) {
        this.fileID = fileID;
        this.checksum = checksum;
        this.path = path;
    }
    
    public String getFileID() {
        return fileID;
    }
    
    public String getChecksum() {
        return checksum;
    }
    
    public String getPath() {
        return path;
    }
    
    public String toString() {
        return path;
    }
}
