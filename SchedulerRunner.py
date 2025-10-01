import os
import sys
import subprocess
import requests

# --- Configuration ---
GITHUB_RAW_URL = "https://raw.githubusercontent.com/tamman86/WCK-Scheduler/refs/heads/master/SchedulerGUI.py"
LOCAL_SCRIPT_NAME = "SchedulerGUI.py"


def download_and_run_script():
    """Downloads the latest script version and executes it."""
    print("Checking for the freshest script version...")

    try:
        # 1. Download the latest script from GitHub
        response = requests.get(GITHUB_RAW_URL, timeout=10)
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)

        # 2. Save the script locally
        with open(LOCAL_SCRIPT_NAME, 'w', encoding='utf-8') as f:
            f.write(response.text)

        print(f"Successfully downloaded the latest version: {LOCAL_SCRIPT_NAME}")

        # 3. Execute the downloaded script
        # We need to execute the downloaded script using the Python interpreter
        # that is *bundled within this very executable*.
        # sys.executable is the path to the current running executable (your client_runner.exe)

        print("Launching the main application...")

        # Use subprocess.run to execute the main script via the bundled Python
        # Pass any command-line arguments the user provided to the main script
        result = subprocess.run([sys.executable, LOCAL_SCRIPT_NAME] + sys.argv[1:], check=True)

        print("Application finished.")
        sys.exit(result.returncode)

    except requests.exceptions.RequestException as e:
        print(f"Error fetching script from GitHub: {e}")
        print("Attempting to run the existing local version (if available)...")
        # Fallback: Run the old version if it exists
        if os.path.exists(LOCAL_SCRIPT_NAME):
            subprocess.run([sys.executable, LOCAL_SCRIPT_NAME] + sys.argv[1:])
        else:
            print("No local script found. Cannot run.")
            sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f"The main script failed with return code {e.returncode}.")
        sys.exit(e.returncode)


if __name__ == "__main__":
    download_and_run_script()