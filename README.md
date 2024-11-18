**[Installation & Requirements](requirements.txt)**

## **Setting up**

* Create a virtual environment

```
python -m venv myenv
```

* Activate virtual environment

```
source myenv/bin/activate
```
* Install the required packages

```
pip install -r requirements.txt
```
## **Running the application**
To run the application manually, use the following command:
```
python main.py or python -m main
```
## **Setting up a Cron Job on Linux**
To set up a cron job, follow these steps:
1. Open the crontab editor by running the following command:
```
crontab -e
```
2. Add the following line to schedule the script to run every day at midnight:
```
0 0 * * * /path/to/python /path/to/main.py >> /path/to/logfile.log 2>&1
```
Replace `/path/to/python` with the path to your Python executable, `/path/to/main.py`
with the path to your `main.py` file, and `/path/to/logfile.log` with the path to your log file.
`2>&1` redirects error output to the same log file. `>> /path/to/logfile.log
3. Save and exit the editor.
4. To verify that the cron job is running, check the log file for output.

## **Setting Up a Scheduled Task on Windows**

**Open Task Scheduler:**

* Press `Win + R` to open the Run dialog.
* Type `taskschd.msc` and press `Enter` to open the Task Scheduler.

**Create a New Task:**

* In the Task Scheduler, click on "Create Basic Task" in the right-hand Actions pane.
* Give your task a name (e.g., "Run Python Script") and an optional description. Click `Next`.

**Set the Trigger:**

* Choose "Daily" to run the task every day, and click `Next`.
* Set the start date and time. For midnight, set the time to `12:00:00 AM`.
* Click `Next`.

**Set the Action:**

* Choose "Start a program" and click `Next`.
* In the "Program/script" field, enter the path to your Python executable (e.g., `C:\Path\To\Python\python.exe`).
* In the "Add arguments (optional)" field, enter the path to your script (e.g., `C:\Path\To\YourScript\main.py`).
* If you want to redirect output to a log file, you can add the following to the "Add arguments" field:

```
C:\Path\To\YourScript\main.py >> C:\Path\To\YourLogFile\logfile.log 2>&1
```

* Click Next.

**Finish the Task:**

* Review your settings and click `Finish` to create the task.

**Verify the Task:**

* You can check the Task Scheduler Library to see your newly created task.
* You can also check the log file you specified to see if the script is running as expected.