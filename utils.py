"""
Utility functions for CSV merging.
This module contains helpers for file operations and text processing.
"""

import re
from pathlib import Path
from typing import List, Optional
from natsort import natsorted


def extract_ex_number(filename: str) -> Optional[int]:
    """
    Extract the exercise number from a filename.
    
    Example:
        "DO200B Ex2 5min.csv" -> 2
        "DO200B_B_Ex10.csv" -> 10
        "random_file.csv" -> None
    
    Args:
        filename: The name of the file to parse
        
    Returns:
        The exercise number if found, None otherwise
    """
    # Use regex to find "ex" followed by digits, case-insensitive
    # \d+ means "one or more digits"
    match = re.search(r'ex(\d+)', filename.lower())
    if match:
        return int(match.group(1))
    return None


def natural_sort_files(files: List[Path]) -> List[Path]:
    """
    Sort files naturally by their Ex number.
    
    Files without Ex numbers are placed at the end.
    
    Args:
        files: List of Path objects to sort
        
    Returns:
        Sorted list of Path objects
    """
    def sort_key(file_path: Path) -> tuple:
        ex_num = extract_ex_number(file_path.name)
        # Files with Ex numbers come first, sorted by number
        # Files without come last, sorted alphabetically
        if ex_num is not None:
            return (0, ex_num)
        else:
            return (1, file_path.name.lower())
    
    return sorted(files, key=sort_key)


def normalize_column_name(col_name: str) -> str:
    """
    Normalize column names for comparison.
    
    Removes whitespace and converts to lowercase.
    
    Example:
        "Channel Name" -> "channelname"
        "  X " -> "x"
        
    Args:
        col_name: The column name to normalize
        
    Returns:
        Normalized column name
    """
    return ''.join(col_name.lower().split())


def find_matching_column(target: str, available_columns: List[str]) -> Optional[str]:
    """
    Find a column that contains the target string (case-insensitive).
    
    Args:
        target: The string to search for (e.g., "x", "y", "z")
        available_columns: List of available column names
        
    Returns:
        The matching column name or None if not found
    """
    target_normalized = normalize_column_name(target)
    
    for col in available_columns:
        col_normalized = normalize_column_name(col)
        if target_normalized in col_normalized:
            return col
    
    return None