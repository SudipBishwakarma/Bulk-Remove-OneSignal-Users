# Bulk Remove OneSignal Users
Bulk remove OneSignal subscribed users blazingly faster with parallel processing.

## Description
This script makes use of pythons multiprocessing module for spawning N-number of child processes in parallel to execute auto-generated csv files.
Pythons threading module is used to fetch processed rows from queue and pass it to the logger.

Pandas is used for csv manipulation and Numpy for handy vectorization without explicit for loops.

## Usage
'file.py' is the main file to execute and depends on 'settings.py' file for api credentials and file to import.

File 'settings_sample.py' is identical to 'settings.py'.

Set CHUNK_SIZE to your desired figure. For instance, if the import file has 18K rows, then CHUNK_SIZE of 500 will create:
```
18000 / 500 = 36
36 files with 500 rows each and start 36 child processes to process generated files in parallel.
```

If the CHUNK_SIZE is lower, more files will get generated as well as the number of processes.
One downside to this is, your computer might become slower due to processes consuming the CPU.

Adjust CHUNK_SIZE based on optimal value and make sure you're not running any cpu intensive tasks before executing this script.

```
$ python3 file.py
```


Pull requests and feature improvements are most welcome.
