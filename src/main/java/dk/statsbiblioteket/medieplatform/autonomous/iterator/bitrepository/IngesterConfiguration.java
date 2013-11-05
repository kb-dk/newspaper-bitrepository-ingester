package dk.statsbiblioteket.medieplatform.autonomous.iterator.bitrepository;

public class IngesterConfiguration {
    private final String componentID;
    private final String collectionID;
    private final String SettingsDir;
    private final String certificateLocation;
    private final int maxNumberOfParallelPuts;

    public IngesterConfiguration(
            String componentID, String collectionID, String settingsDir, String certificateLocation, int maxNumberOfParallelPuts) {
        this.componentID = componentID;
        this.collectionID = collectionID;
        SettingsDir = settingsDir;
        this.certificateLocation = certificateLocation;
        this.maxNumberOfParallelPuts = maxNumberOfParallelPuts;
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
}
