package dk.statsbiblioteket.medieplatform.autonomous.iterator.bitrepository;

import java.net.URL;

import dk.statsbiblioteket.medieplatform.autonomous.iterator.common.ParsingEvent;
import dk.statsbiblioteket.medieplatform.autonomous.iterator.common.TreeIterator;
import dk.statsbiblioteket.medieplatform.autonomous.iterator.filesystem.FileAttributeParsingEvent;

/**
 * Concrete implementation of <code>IngestableFileLocator</code> defining how to find the jp2 files to store for
 * a newspaper batch.
 */
public abstract class AbstractImageLocator implements IngestableFileLocator {
    private final TreeIterator treeIterator;

    public AbstractImageLocator(TreeIterator treeIterator) {
        this.treeIterator = treeIterator;
    }

    @Override
    public IngestableFile nextFile() {
        while (treeIterator.hasNext()) {
            ParsingEvent event = treeIterator.next();
            if (isIngestableNode(event)) {
                FileAttributeParsingEvent fileEvent = (FileAttributeParsingEvent)event;
                return createIngestableFile(fileEvent);
            }
        }
        return null;
    }

    protected abstract boolean isIngestableNode(ParsingEvent event);
    protected abstract IngestableFile createIngestableFile(FileAttributeParsingEvent fileEvent);
    protected abstract String getFileID(FileAttributeParsingEvent event);
    protected abstract URL getFileUrl(FileAttributeParsingEvent event);
}

