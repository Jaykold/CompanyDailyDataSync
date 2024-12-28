import azure.functions as func
import os
import logging
import sys
import asyncio

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

os.chdir(project_root)

from main import main

app = func.FunctionApp()

@app.timer_trigger(schedule="*/20 * * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def MyTimer(myTimer: func.TimerRequest) -> None:
    
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')

    try:
        logging.info(f"Is main async? {asyncio.iscoroutinefunction(main)}")

        loop = asyncio.get_event_loop()
        
        # Ensure the loop is running and execute async main logic
        if loop.is_running():
            # If loop is already running (in case of other async tasks), we can schedule it in the background
            asyncio.ensure_future(main())
        else:
            # If no loop is running, create and run a new event loop to execute async code
            loop.run_until_complete(main())
        logging.info("Data ingestion executed successfully")
    except Exception as e:
        logging.error("An error occured during data ingestion: %s", str(e))