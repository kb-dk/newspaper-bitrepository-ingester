package dk.statsbiblioteket.newspaper.bitrepository.ingester;

import java.util.Properties;

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
    public BitrepositoryIngesterComponent(Properties properties) {
        super(properties);
    }

    @Override
    public String getComponentName() {
        return getClass().getSimpleName();
    }

    @Override
    public String getComponentVersion() {
        return "0.1";
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
                getProperties().getProperty(CERTIFICATE_PROPERTY),
                Integer.parseInt(getProperties().getProperty(MAX_NUMBER_OF_PARALLEL_PUTS_PROPERTY)));
        Settings settings = loadSettings(configuration);
        PutFileClient ingestClient = createPutFileClient(configuration, settings);
        TreeIngester ingester = new TreeIngester(
                configuration.getCollectionID(),
                new BatchImageLocator(createIterator(batch),
                getProperties().getProperty(URL_TO_BATCH_DIR_PROPERTY)),
                ingestClient,
                resultCollector);
        ingester.performIngest();
        ingester.shutdown();
    }

    protected PutFileClient createPutFileClient(IngesterConfiguration configuration, Settings settings) {
        PermissionStore permissionStore = new PermissionStore();
        MessageAuthenticator authenticator = new BasicMessageAuthenticator(permissionStore);
        MessageSigner signer = new BasicMessageSigner();
        OperationAuthorizor authorizer = new BasicOperationAuthorizor(permissionStore);
        org.bitrepository.protocol.security.SecurityManager securityManager =
                new BasicSecurityManager(settings.getRepositorySettings(),
                getProperties().getProperty(CERTIFICATE_PROPERTY),
                authenticator, signer, authorizer, permissionStore, getProperties().getProperty(COMPONENTID_PROPERTY));
        return ModifyComponentFactory.getInstance().retrievePutClient(
                settings, securityManager,
                getProperties().getProperty(COMPONENTID_PROPERTY));
    }

    protected Settings loadSettings(IngesterConfiguration configuration) {
        SettingsProvider settingsLoader = new SettingsProvider(
                new XMLFileSettingsLoader(configuration.getSettingsDir()),
                configuration.getComponentID());
        return settingsLoader.getSettings();
    }
}
