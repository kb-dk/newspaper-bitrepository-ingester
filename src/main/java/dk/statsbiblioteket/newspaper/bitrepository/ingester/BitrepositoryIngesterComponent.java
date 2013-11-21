package dk.statsbiblioteket.newspaper.bitrepository.ingester;

import java.net.MalformedURLException;
import java.util.Properties;

import javax.xml.bind.JAXBException;

import dk.statsbiblioteket.doms.central.connectors.EnhancedFedora;
import dk.statsbiblioteket.doms.central.connectors.EnhancedFedoraImpl;
import dk.statsbiblioteket.doms.central.connectors.fedora.pidGenerator.PIDGeneratorException;
import dk.statsbiblioteket.doms.webservices.authentication.Credentials;
import dk.statsbiblioteket.medieplatform.autonomous.AbstractRunnableComponent;
import dk.statsbiblioteket.medieplatform.autonomous.Batch;
import dk.statsbiblioteket.medieplatform.autonomous.ResultCollector;
import dk.statsbiblioteket.medieplatform.autonomous.iterator.bitrepository.IngesterConfiguration;
import dk.statsbiblioteket.medieplatform.autonomous.iterator.bitrepository.TreeIngester;

import org.bitrepository.common.settings.Settings;
import org.bitrepository.common.settings.SettingsProvider;
import org.bitrepository.common.settings.XMLFileSettingsLoader;
import org.bitrepository.modify.ModifyComponentFactory;
import org.bitrepository.modify.putfile.PutFileClient;
import org.bitrepository.protocol.security.BasicMessageAuthenticator;
import org.bitrepository.protocol.security.BasicMessageSigner;
import org.bitrepository.protocol.security.BasicOperationAuthorizor;
import org.bitrepository.protocol.security.BasicSecurityManager;
import org.bitrepository.protocol.security.MessageAuthenticator;
import org.bitrepository.protocol.security.MessageSigner;
import org.bitrepository.protocol.security.OperationAuthorizor;
import org.bitrepository.protocol.security.PermissionStore;

/**
 * Checks the directory structure of a batch. This should run both at Ninestars and at SB.
 */
public class BitrepositoryIngesterComponent extends AbstractRunnableComponent {
    public static final String COLLECTIONID_PROPERTY="bitrepository.ingester.collectionid";
    public static final String COMPONENTID_PROPERTY="bitrepository.ingester.componentid";
    public static final String SETTINGS_DIR_PROPERTY="bitrepository.ingester.settingsdir";
    public static final String CERTIFICATE_PROPERTY="bitrepository.ingester.certificate";
    public static final String URL_TO_BATCH_DIR_PROPERTY="bitrepository.ingester.urltobatchdir";
    public static final String MAX_NUMBER_OF_PARALLEL_PUTS_PROPERTY="bitrepository.ingester.numberofparrallelPuts";
    public static final String DOMS_CENTRAL_URL_PROPERTY = "domsUrl";
    public static final String DOMS_USER_PROPERTY = "domsUser";
    public static final String DOMS_PASS_PROPERTY = "domsPass";
    public static final String BITMAG_BASEURL_PROPERTY = "bitrepository.ingester.baseurl";
    
    public BitrepositoryIngesterComponent(Properties properties) {
        super(properties);
    }

    @Override
    public String getEventID() {
        return "Batch ingested";
    }

    /**
     * Ingests all the jp2 files for the indicated batch into the configured bit repository the indicated batch.
     */
    @Override
    public void doWorkOnBatch(Batch batch, ResultCollector resultCollector) throws Exception {
        IngesterConfiguration configuration = new IngesterConfiguration(
                getProperties().getProperty(COMPONENTID_PROPERTY),
                getProperties().getProperty(COLLECTIONID_PROPERTY),
                getProperties().getProperty(SETTINGS_DIR_PROPERTY),
                getProperties().getProperty(SETTINGS_DIR_PROPERTY) + "/" + getProperties().getProperty(CERTIFICATE_PROPERTY),
                Integer.parseInt(getProperties().getProperty(MAX_NUMBER_OF_PARALLEL_PUTS_PROPERTY)),
                getProperties().getProperty(DOMS_CENTRAL_URL_PROPERTY),
                getProperties().getProperty(DOMS_USER_PROPERTY), 
                getProperties().getProperty(DOMS_PASS_PROPERTY), 
                getProperties().getProperty(BITMAG_BASEURL_PROPERTY));
        Settings settings = loadSettings(configuration);
        PutFileClient ingestClient = createPutFileClient(configuration, settings);
        DomsJP2FileUrlRegister urlRegister = new DomsJP2FileUrlRegister(getEnhancedFedora(configuration));
        TreeIngester ingester = new TreeIngester(
                configuration.getCollectionID(),
                settings.getRepositorySettings().getClientSettings().getOperationTimeout().longValue(),
                new BatchImageLocator(createIterator(batch),
                getProperties().getProperty(URL_TO_BATCH_DIR_PROPERTY)),
                ingestClient,
                resultCollector, configuration.getMaxNumberOfParallelPuts(),
                urlRegister,
                configuration.getBitmagBaseUrl());
        ingester.performIngest();
        ingester.shutdown();
    }

    protected EnhancedFedora createEnhancedFedora(IngesterConfiguration configuration) {
        Credentials creds = new Credentials(configuration.getDomsUser(), configuration.getDomsPass());
        try {
            return new EnhancedFedoraImpl(creds, configuration.getDomsUrl(), null, null);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
    
    protected EnhancedFedora getEnhancedFedora(IngesterConfiguration ingesterConfig) {
        Credentials creds = new Credentials(ingesterConfig.getDomsUser(), ingesterConfig.getDomsPass());
        try {
            EnhancedFedoraImpl fedora = new EnhancedFedoraImpl(creds, ingesterConfig.getDomsUrl(), null, null);
            return fedora;
        } catch (MalformedURLException | PIDGeneratorException | JAXBException e) {
            throw new RuntimeException("Failed to get a connection to DOMS.", e);
        }
    }
    
    /**
     * Creates a default put file client. May be overridden by specialized BitrepositoryIngesterComponents.
     */
    protected PutFileClient createPutFileClient(IngesterConfiguration configuration, Settings settings) {
        PermissionStore permissionStore = new PermissionStore();
        MessageAuthenticator authenticator = new BasicMessageAuthenticator(permissionStore);
        MessageSigner signer = new BasicMessageSigner();
        OperationAuthorizor authorizer = new BasicOperationAuthorizor(permissionStore);
        org.bitrepository.protocol.security.SecurityManager securityManager =
                new BasicSecurityManager(settings.getRepositorySettings(),
                configuration.getCertificateLocation(),
                authenticator, signer, authorizer, permissionStore, getProperties().getProperty(COMPONENTID_PROPERTY));
        return ModifyComponentFactory.getInstance().retrievePutClient(
                settings, securityManager,
                getProperties().getProperty(COMPONENTID_PROPERTY));
    }

    /**
     * Load settings from disk. May be overridden by specialized custom functionality.
     */
    protected Settings loadSettings(IngesterConfiguration configuration) {
        SettingsProvider settingsLoader = new SettingsProvider(
                new XMLFileSettingsLoader(configuration.getSettingsDir()),
                configuration.getComponentID());
        return settingsLoader.getSettings();
    }
}
