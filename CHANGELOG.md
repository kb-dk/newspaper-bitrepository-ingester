### 1.3
* Update component to newspaper-batch-event-framework to make it quiet
* Add details to failure report when components fail. 

### 1.2
* Make the ingester skip a batch if it cannot successfully force the files online
* Update to depend on newspaper-batch-event-framework 1.4

### 1.1
* Updated to use Bitrepository 1.1.1 (to prevent thread leaks)
* Updated to autonomous component framework 1.3
* Limit the amount of heap space the ingester is allowed to use
* Remove messages on stdout

### 1.0
* Updated to use Bitrepository 1.1
* After a file has been ingested into the repository, the file will be registered in the configured doms.
* A batch will be marked as 'forceonline' before ingest is begun. This means the configured 'forceonline' script is called
which should prevent the files for the batch from beingen moved from disk cache to tape.

### 0.1
Initial release

