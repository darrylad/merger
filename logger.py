"""
Logging utilities for the CSV merger.
Handles both console output and file logging.
"""

import sys
from pathlib import Path
from datetime import datetime
from typing import TextIO


class DualOutput:
    """
    A class that writes to both console and a log file simultaneously.
    """
    
    def __init__(self, log_file_path: Path):
        """
        Initialize dual output.
        
        Args:
            log_file_path: Path to the log file
        """
        self.terminal = sys.stdout
        self.log_file = open(log_file_path, 'w', encoding='utf-8')
        
    def write(self, message: str):
        """
        Write message to both terminal and log file.
        
        Args:
            message: The message to write
        """
        self.terminal.write(message)
        self.log_file.write(message)
        
    def flush(self):
        """Flush both outputs."""
        self.terminal.flush()
        self.log_file.flush()
        
    def close(self):
        """Close the log file."""
        self.log_file.close()


class Logger:
    """
    Manages logging for the CSV merger application.
    """
    
    def __init__(self, output_dir: Path):
        """
        Initialize logger with output directory.
        
        Args:
            output_dir: Directory where log file will be saved
        """
        self.output_dir = output_dir
        self.dual_output = None
        self.original_stdout = None
        
    def start(self) -> Path:
        """
        Start logging to file.
        
        Returns:
            Path to the created log file
        """
        # Create log filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file_path = self.output_dir / f"merger_log_{timestamp}.txt"
        
        # Save original stdout
        self.original_stdout = sys.stdout
        
        # Create dual output
        self.dual_output = DualOutput(log_file_path)
        
        # Redirect stdout to dual output
        sys.stdout = self.dual_output
        
        print(f"Logging started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Log file: {log_file_path}")
        print("=" * 60)
        
        return log_file_path
        
    def stop(self):
        """Stop logging and restore original stdout."""
        if self.dual_output:
            print("\n" + "=" * 60)
            print(f"Logging ended at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Restore original stdout
            sys.stdout = self.original_stdout
            
            # Close log file
            self.dual_output.close()
            
            print(f"📄 Log saved to: {self.dual_output.log_file.name}")