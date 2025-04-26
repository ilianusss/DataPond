import subprocess
import sys

def main():
    try:
        print("Launching Mini Financial Data Lake Explorer...")
        subprocess.run(["streamlit", "run", "dashboard/dashboard.py"], 
                      check=True)
    except KeyboardInterrupt:
        print("\nExiting application...")
        return 0
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
