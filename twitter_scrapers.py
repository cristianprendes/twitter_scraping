import os
import time
import subprocess
import multiprocessing
from datetime import datetime

def run_script(script_path, log_path):
    with open(log_path, "a") as log_file:
        subprocess.run(["python3", "-u", script_path], stdout=log_file, stderr=subprocess.STDOUT)

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))

    scripts = [
        os.path.join(base_dir, "set1", "twitter_tweets_scraping_1.py"),
        os.path.join(base_dir, "set2", "twitter_tweets_scraping_2.py"),
        os.path.join(base_dir, "set3", "twitter_tweets_scraping_3.py"),
        os.path.join(base_dir, "set4", "twitter_tweets_scraping_4.py"),
        os.path.join(base_dir, "set5", "twitter_tweets_scraping_5.py"),
        os.path.join(base_dir, "set6", "twitter_tweets_scraping_6.py"),
        os.path.join(base_dir, "set7", "twitter_tweets_scraping_7.py")
    ]
    
    processes = []
    
    for script in scripts:
        script_dir = os.path.dirname(script)
        script_name = os.path.basename(script)
        log_path = os.path.join(script_dir, f"log_{script_name.split('.')[0]}.log")
        process = multiprocessing.Process(target=run_script, args=(script, log_path))
        processes.append(process)
        process.start()
    
    for process in processes:
        process.join()

    print("All tweet scraping scripts are completed\n")

if __name__ == "__main__":
    while True:
        session_start_time = datetime.fromtimestamp(time.time())
        print(f"session_start_time = {session_start_time}")

        main()

        session_end_time = datetime.fromtimestamp(time.time())
        print(f"session_end_time = {session_end_time}")
        print(f"total_time = {session_end_time - session_start_time}")
        print("-----------------------------------------------------\n")
        print("Sleeping for 2 hours before next loop begins to reduce 429 response\n")
        print("-----------------------------------------------------")
        time.sleep(7200)