import sys
import subprocess

def install_required_modules():
    print("Installing yfinance module using pip")
    subprocess.run([sys.executable, '-m', 'pip', 'install', 'yfinance'])
    # TODO: MAKE THIS METHOD ABLE TO INSTALL modules USING ANY given REQUIREMENT CONFIG, not just yfinance 
    # TODO: MAKE THIS METHOD ABLE TO INSTALL ANY AND ALL MISSING MODULES AFTER AN EXCEPTION IS RAISED

def 