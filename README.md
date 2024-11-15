**[Installation & Requirements](requirements.txt)**

**Setting up**
```
pip install -r requirements.txt
```
**Running the application**
To run the application manually, use the following command:
```
python main.py or python -m main
```
**Setting up a Cron Job on Linux**
To set up a cron job, follow these steps:
1. Open the crontab editor by running the following command:
```
crontab -e
```
2. Add the following line to schedule the script to run every 5 minutes:
```
0 0 * * * /path/to/python /path/to/main.py >> /path/to/logfile.log 2>&1
```
Replace `/path/to/python` with the path to your Python executable, `/path/to/main.py`
with the path to your `main.py` file, and `/path/to/logfile.log` with the path to your log file.
`2>&1` redirects error output to the same log file. `>> /path/to/logfile.log
3. Save and exit the editor.
4. To verify that the cron job is running, check the log file for output.