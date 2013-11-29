### 0.1
Initial release

### 1.0
Updated to use Bitrepository 1.1
After a file has been ingested into the repository, the file will be registered in the configured doms.
A batch will be marked as 'forceonline' before ingest is begun. This means the configured 'forceonline' script is called
which should prevent the files for the batch from beingen moved from disk cache to tape.
