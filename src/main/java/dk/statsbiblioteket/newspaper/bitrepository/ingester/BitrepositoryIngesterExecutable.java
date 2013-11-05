package dk.statsbiblioteket.newspaper.bitrepository.ingester;

import java.util.Map;
import java.util.Properties;

import dk.statsbiblioteket.medieplatform.autonomous.AutonomousComponentUtils;
import dk.statsbiblioteket.medieplatform.autonomous.RunnableComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** AutonomousComponent wrapper for the BitrepositoryIngester. */
public class BitrepositoryIngesterExecutable {
    private static Logger log = LoggerFactory.getLogger(BitrepositoryIngesterExecutable.class);

    /**
     * Main method, so it can be started as a command line tool.
     *
     * @param args the arguments.
     *
     * @throws Exception
     * @see dk.statsbiblioteket.medieplatform.autonomous.AutonomousComponentUtils#parseArgs(String[])
     */
    public static void main(String[] args)
            throws
            Exception {
        log.info("Starting with args {}", args);
        Properties properties = AutonomousComponentUtils.parseArgs(args);
        RunnableComponent component = new BitrepositoryIngesterComponent(properties);
        Map<String, Boolean> result = AutonomousComponentUtils.startAutonomousComponent(properties, component);

        AutonomousComponentUtils.printResults(result);
    }
}
