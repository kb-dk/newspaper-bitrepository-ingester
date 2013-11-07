package dk.statsbiblioteket.medieplatform.autonomous.iterator.bitrepository;

public class IngesterConfiguration {
    private final String componentID;
    private final String collectionID;
    private final String SettingsDir;
    private final String certificateLocation;
    private final int maxNumberOfParallelPuts;
    private final String domsUrl;
    private final String domsUser;
    private final String domsPass;

    public IngesterConfiguration(String componentID, String collectionID, String settingsDir, 
            String certificateLocation, int maxNumberOfParallelPuts, String domsUrl, 
            String domsUser, String domsPass) {
        this.componentID = componentID;
        this.collectionID = collectionID;
        SettingsDir = settingsDir;
        this.certificateLocation = certificateLocation;
        this.maxNumberOfParallelPuts = maxNumberOfParallelPuts;
        this.domsUrl = domsUrl;
        this.domsUser = domsUser;
        this.domsPass = domsPass;
    }

    public String getComponentID() {
        return componentID;
    }

    public String getSettingsDir() {
        return SettingsDir;
    }

    public String getCertificateLocation() {
        return certificateLocation;
    }

    public int getMaxNumberOfParallelPuts() {
        return maxNumberOfParallelPuts;
    }

    public String getCollectionID() {
        return collectionID;
    }
    
    public String getDomsUrl() {
        return domsUrl;
    }
    
    public String getDomsUser() {
        return domsUser;
    }
    
    public String getDomsPass() {
        return domsPass;
    }
    
}
