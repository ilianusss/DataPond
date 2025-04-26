import subprocess
import sys
import re
from pathlib import Path

def validate_email(email):
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None

def prompt_for_email():
    email = input("Email address for SEC API: ")
    while not validate_email(email):
        email = input("Invalid address, enter again: ")
    return email

def update_user_agent(email):
    fundamentals_path = Path("scripts/fundamentals.py")
    if not fundamentals_path.exists():
        print("[ERR] Could not find fundamentals.py")
        return False
    
    content = fundamentals_path.read_text()
    updated_content = re.sub(
        r'"User-Agent": "DataPond/1\.1 \([^)]+\)"',
        f'"User-Agent": "DataPond/1.1 ({email})"',
        content
    )
    
    fundamentals_path.write_text(updated_content)
    return True

def main():
    try:
        email = prompt_for_email()
        update_user_agent(email)
        
        print("Launching DataPond...")
        subprocess.run(["streamlit", "run", "dashboard/dashboard.py"], 
                      check=True)
    except KeyboardInterrupt:
        print("\nExiting application...")
        return 0
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
