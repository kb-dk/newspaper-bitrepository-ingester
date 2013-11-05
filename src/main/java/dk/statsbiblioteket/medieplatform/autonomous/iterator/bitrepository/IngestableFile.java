package dk.statsbiblioteket.medieplatform.autonomous.iterator.bitrepository;

import java.net.URL;

import org.bitrepository.bitrepositoryelements.ChecksumDataForFileTYPE;

/**
 * Contains all relevant attributes need for ingest of a file into a bitrepository.
 */
public class IngestableFile {
    private final String FileID;
    private final URL url;
    private final ChecksumDataForFileTYPE checksum;
    private final Long fileSize;

    public IngestableFile(String fileID, URL url, ChecksumDataForFileTYPE checksum, Long fileSize) {
        FileID = fileID;
        this.url = url;
        this.checksum = checksum;
        this.fileSize = fileSize;
    }

    public String getFileID() {
        return FileID;
    }

    public URL getUrl() {
        return url;
    }

    public ChecksumDataForFileTYPE getChecksum() {
        return checksum;
    }

    public Long getFileSize() {
        return fileSize;
    }

    @Override
    public String toString() {
        return "IngestableFile{" +
                "FileID='" + FileID + '\'' +
                ", url=" + url +
                ", checksum=" + checksum +
                ", fileSize=" + fileSize +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IngestableFile)) return false;

        IngestableFile that = (IngestableFile) o;

        if (!FileID.equals(that.FileID)) return false;
        if (!checksum.equals(that.checksum)) return false;
        if (!fileSize.equals(that.fileSize)) return false;
        if (!url.equals(that.url)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = FileID.hashCode();
        result = 31 * result + url.hashCode();
        result = 31 * result + checksum.hashCode();
        result = 31 * result + fileSize.hashCode();
        return result;
    }
}
