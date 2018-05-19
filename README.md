# file-processor

`file-processsor.py` is an easy file processing tool that executes a given command on each file in a given directory including it's sub-directories.

It retries files in case a command fails (exit code != 0) or does not return at all after a pre-defined timeout.
After successfully processing, the file can be either deleted, moved or renamed.

`file-processor.py` keeps running as long as there are files left unprocessed or for a defined amount of seconds (using --runtime). While running, it periodically checks for new files appearing in the working directory.

While `file-processor.py` is not multi-threaded by itself, it supports multiple concurrent processes of itself running on the same set of files.

Adaptive spawning can be achieved by running `file-processor.py` as a cron in combination with `--runtime` and `--max-concurrency`.


## Installation

```bash
# wget -O- https://raw.githubusercontent.com/maurice2k/file-processor/master/file-processor.py >/usr/local/bin/file-processor.py
# chmod +x /usr/local/bin/file-processor.py
```


## Sample usage

### Example 1: Import MySQL dumps
Runs `mysql` on each SQL dump, sorted by name. The file is piped into the `mysql` command. 
```bash
$ file-processor.py ~/sqldumps/ mysql --sort name
```

### Example 2: Push all mails to an API
Runs `curl` on each new mail file. `{}` is being replaced with the filename to process before executing curl.
```
*/1 * * * * file-processor.py ~/maildir/ 'curl -XPOST https://.../mail-import -d@{}' --max-runtime 60 --max-concurrency 5 --delete
```


## Available command line options
```
usage: file-processor.py [-h] [--verbose] [--sort {mtime,name}]
                         [--sort-reverse] [--max-concurrency MAX_CONCURRENCY]
                         [--max-runtime MAX_RUNTIME]
                         [--process-timeout PROCESS_TIMEOUT]
                         [--move-to MOVE_TO] [--delete]
                         <DIRECTORY> <COMMAND>

positional arguments:
  <DIRECTORY>           Working directory
  <COMMAND>             Command to run on each file. Use {} as a placeholder
                        for the file path. If no placeholder is found, the
                        file's content is piped to STDIN of the given command.
                        On success the command MUST return with exit code 0.
                        All other exit codes will be considered as failure and
                        will trigger a retry.

optional arguments:
  -h, --help            show this help message and exit
  --verbose, -v         Level of verbosity (default: None)
  --sort {mtime,name}, -s {mtime,name}
                        Sort by modification time or name (default: mtime)
  --sort-reverse, -r    Sort in reverse order (default: False)
  --max-concurrency MAX_CONCURRENCY, -c MAX_CONCURRENCY
                        Max. number of parallel file processors on the given
                        directory (default: 10)
  --max-runtime MAX_RUNTIME
                        Maximum runtime in seconds (default: 0)
  --process-timeout PROCESS_TIMEOUT
                        Time in seconds after which a file in process will be
                        considered as failed (and will be retried). This is
                        dependent on how long the processing command (--cmd)
                        will usually take to process a single file. (default:
                        300)
  --move-to MOVE_TO     Move files to given location (instead of renaming to
                        .fp-done-...) after being successfully processed.
                        Files are moved without sub-directories and existing
                        files will be overwritten. (default: None)
  --delete              Delete files (instead of renaming to .fp-done-...)
                        after being successfully processed. (default: False)
```
