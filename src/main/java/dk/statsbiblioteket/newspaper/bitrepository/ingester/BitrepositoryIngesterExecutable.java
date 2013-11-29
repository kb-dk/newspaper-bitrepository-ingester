package dk.statsbiblioteket.newspaper.bitrepository.ingester;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import dk.statsbiblioteket.medieplatform.autonomous.AutonomousComponentUtils;
import dk.statsbiblioteket.medieplatform.autonomous.CallResult;
import dk.statsbiblioteket.medieplatform.autonomous.RunnableComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** AutonomousComponent wrapper for the BitrepositoryIngester. */
public class BitrepositoryIngesterExecutable {
    private static Logger log = LoggerFactory.getLogger(BitrepositoryIngesterExecutable.class);

    /**
     * The class must have a main method, so it can be started as a command line tool
     *
     * @param args the arguments.
     *
     * @throws Exception
     * @see AutonomousComponentUtils#parseArgs(String[])
     */
    public static void main(String... args) throws IOException {
        System.exit(doMain(args));
    }

    /**
     * Main method, so it can be started as a command line tool.
     *
     * @param args the arguments.
     *
     * @throws Exception
     * @see dk.statsbiblioteket.medieplatform.autonomous.AutonomousComponentUtils#parseArgs(String[])
     */
    public static int doMain(String[] args) throws IOException {
        log.info("Starting with args {}", args);
        Properties properties = parseArgs(args);
        RunnableComponent component = new BitrepositoryIngesterComponent(properties);

        CallResult result = AutonomousComponentUtils.startAutonomousComponent(properties, component);
        System.out.println("result was: " + result);
        return result.containsFailures();
        
    }

    /**
     * Sample method to parse properties. This is probably not the best way to do this
     * It makes a new properties, with the system defaults. It then scan the args for a the string "-c". If found
     * it expects the next arg to be a path to a properties file.
     *
     * @param args the command line args
     *
     * @return as a properties
     * @throws java.io.IOException if the properties file could not be read
     */
    public static Properties parseArgs(String[] args) throws IOException {
        Properties properties = new Properties(System.getProperties());
        for (int i = 0;
             i < args.length;
             i++) {
            String arg = args[i];
            if (arg.equals("-c")) {
                String configFileName = args[i + 1];
                properties.load(new FileInputStream(configFileName));
                File configFile = new File(configFileName);
                properties.setProperty(
                        BitrepositoryIngesterComponent.SETTINGS_DIR_PROPERTY,
                        configFile.getParentFile().getAbsolutePath());
            }
        }
        return properties;
    }
}
