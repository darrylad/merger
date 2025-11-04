"""
Core CSV merging functionality.
Handles reading, processing, and merging CSV files by class.
"""

import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from utils import natural_sort_files, find_matching_column, extract_ex_number
from logger import Logger


class CSVMerger:
    """
    Merges CSV files containing exercise data by class.
    
    The merger expects a directory structure where:
    - First-level subdirectories are classes
    - CSV files within subdirectories are exercise data
    """
    
    def __init__(self, root_path: str):
        """
        Initialize the merger.
        
        Args:
            root_path: Path to the root directory containing class folders
        """
        self.root_path = Path(root_path)
        
        if not self.root_path.exists():
            raise FileNotFoundError(f"Root path does not exist: {root_path}")
        
        if not self.root_path.is_dir():
            raise NotADirectoryError(f"Root path is not a directory: {root_path}")
        
        # Store the project directory (where merger.py is located)
        self.project_dir = Path(__file__).parent
        self.logger = None
    
    def get_class_folders(self) -> List[Path]:
        """
        Get all first-level subdirectories (classes).
        
        Returns:
            List of Path objects for each class folder
        """
        # Use list comprehension to filter directories
        # Path.iterdir() gives all items in the directory
        # We filter to keep only directories (not files)
        return [item for item in self.root_path.iterdir() if item.is_dir()]
    
    def get_csv_files(self, class_folder: Path) -> List[Path]:
        """
        Get all CSV files in a class folder, naturally sorted.
        
        Args:
            class_folder: Path to the class folder
            
        Returns:
            Sorted list of CSV file paths
        """
        # glob('*.csv') finds all files ending with .csv
        csv_files = list(class_folder.glob('*.csv'))
        return natural_sort_files(csv_files)
    
    def read_csv_safely(self, file_path: Path) -> Tuple[pd.DataFrame, bool]:
        """
        Read a CSV file with error handling.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            Tuple of (DataFrame, success_flag)
        """
        try:
            # Read CSV with maximum precision
            # float_precision='high' prevents rounding errors
            df = pd.read_csv(file_path, float_precision='high')
            return df, True
        except Exception as e:
            print(f"  ⚠️  Error reading {file_path.name}: {e}")
            return pd.DataFrame(), False
    
    def merge_dataframes(self, dfs: List[pd.DataFrame], 
                        file_names: List[str]) -> pd.DataFrame:
        """
        Merge multiple DataFrames vertically (row-wise) with continuous time.
        
        Each subsequent DataFrame has its time values offset by the last time
        value from the accumulated data PLUS the sampling interval, making the 
        time column continuous without overlapping timestamps.
        
        Args:
            dfs: List of DataFrames to merge
            file_names: List of original file names (for labeling)
            
        Returns:
            Merged DataFrame with continuous time
        """
        if not dfs:
            return pd.DataFrame()
        
        # Find the time column in the first DataFrame
        first_df = dfs[0]
        time_col = find_matching_column('channel', first_df.columns)
        
        if time_col is None:
            print("  ⚠️  Warning: No time/channel column found")
            return pd.DataFrame()
        
        print(f"  🕒 Time column identified: '{time_col}'")
        
        # Start with the first DataFrame
        result = dfs[0].copy()
        
        # Calculate sampling interval from the first DataFrame
        # This is the difference between the last two time values
        if len(result) >= 2:
            sampling_interval = result[time_col].iloc[-1] - result[time_col].iloc[-2]
        else:
            # If only one row, use a default or the time value itself
            sampling_interval = result[time_col].iloc[-1] if len(result) == 1 else 0
        
        print(f"  📊 Ex1: {len(result)} rows, time range: {result[time_col].iloc[0]:.6f} to {result[time_col].iloc[-1]:.6f}, sampling interval: {sampling_interval:.6f}")
        
        # Current offset starts at last time + sampling interval
        current_time_offset = result[time_col].iloc[-1] + sampling_interval
        
        # For each subsequent DataFrame
        for idx, (df, file_name) in enumerate(zip(dfs[1:], file_names[1:]), start=2):
            ex_num = extract_ex_number(file_name)
            ex_label = f"Ex{ex_num}" if ex_num else f"Ex{idx}"
            
            # Create a copy to avoid modifying the original
            df_copy = df.copy()
            
            # Find time column in this DataFrame (might have different case/spacing)
            df_time_col = find_matching_column('channel', df_copy.columns)
            
            if df_time_col is None:
                print(f"  ⚠️  Warning: No time column in {file_name}, skipping...")
                continue
            
            # Get the first timestamp in this DataFrame
            first_time_in_df = df_copy[df_time_col].iloc[0]
            
            # Add offset to make time continuous
            # The offset is: last_time_from_previous + sampling_interval
            df_copy[df_time_col] = df_copy[df_time_col] - first_time_in_df + current_time_offset
            
            # Calculate sampling interval for this DataFrame
            if len(df_copy) >= 2:
                current_sampling_interval = df_copy[df_time_col].iloc[-1] - df_copy[df_time_col].iloc[-2]
            else:
                current_sampling_interval = sampling_interval  # Use previous interval as fallback
            
            print(f"  📊 {ex_label}: {len(df_copy)} rows, time range: {df_copy[df_time_col].iloc[0]:.6f} to {df_copy[df_time_col].iloc[-1]:.6f}, sampling interval: {current_sampling_interval:.6f}")
            
            # Update the offset for the next iteration
            # Add the last time value + sampling interval
            current_time_offset = df_copy[df_time_col].iloc[-1] + current_sampling_interval
            
            # Rename columns if they differ from the result DataFrame
            # This ensures column names match for concatenation
            if df_time_col != time_col:
                df_copy.rename(columns={df_time_col: time_col}, inplace=True)
            
            # Append the DataFrame
            result = pd.concat([result, df_copy], ignore_index=True)
        
        return result
    
    def process_class(self, class_folder: Path, output_base_dir: Path) -> Dict:
        """
        Process all CSV files in a class folder.
        
        Args:
            class_folder: Path to the class folder
            output_base_dir: Base output directory
            
        Returns:
            Dictionary containing metadata and merged DataFrame
        """
        class_name = class_folder.name
        print(f"\n{'='*60}")
        print(f"Processing Class: {class_name}")
        print(f"{'='*60}")
        
        csv_files = self.get_csv_files(class_folder)
        
        if not csv_files:
            print(f"  ⚠️  No CSV files found in {class_name}")
            return {'class': class_name, 'files': [], 'merged_df': pd.DataFrame(), 'output_folder': None}
        
        print(f"Found {len(csv_files)} CSV file(s):")
        
        dataframes = []
        file_names = []
        
        # Create output folder for this class
        class_output_folder = output_base_dir / class_name
        class_output_folder.mkdir(parents=True, exist_ok=True)
        
        metadata = {
            'class': class_name,
            'files': [],
            'total_rows': 0,
            'total_columns': 0,
            'output_folder': class_output_folder
        }
        
        # Read each CSV file
        for csv_file in csv_files:
            ex_num = extract_ex_number(csv_file.name)
            ex_label = f"Ex{ex_num}" if ex_num else "Unknown"
            print(f"  📄 {ex_label}: {csv_file.name}")
            
            df, success = self.read_csv_safely(csv_file)
            
            if success:
                dataframes.append(df)
                file_names.append(csv_file.name)
                
                metadata['files'].append({
                    'name': csv_file.name,
                    'ex_number': ex_num,
                    'rows': len(df),
                    'columns': len(df.columns)
                })
        
        # Merge all DataFrames
        if dataframes:
            print(f"\n  🔄 Merging {len(dataframes)} file(s) vertically...")
            merged_df = self.merge_dataframes(dataframes, file_names)
            
            metadata['total_rows'] = len(merged_df)
            metadata['total_columns'] = len(merged_df.columns)
            metadata['merged_df'] = merged_df
            
            print(f"  ✅ Merged DataFrame: {len(merged_df)} rows × {len(merged_df.columns)} columns")
        else:
            print("  ❌ No valid DataFrames to merge")
            metadata['merged_df'] = pd.DataFrame()
        
        return metadata
    
    def run(self, output_dir: Optional[str] = None) -> None:
        """
        Run the merger on all classes and save results.
        
        Args:
            output_dir: Directory to save merged CSV files (default: project_dir/outputs)
        """
        # Set up output directory
        if output_dir is None:
            # Use outputs folder in the project directory
            output_dir = self.project_dir / 'outputs'
        else:
            output_dir = Path(output_dir)
        
        output_dir.mkdir(exist_ok=True)
        
        # Initialize logger
        self.logger = Logger(output_dir)
        log_file = self.logger.start()
        
        try:
            print(f"\n🚀 Starting CSV Merger")
            print(f"Root Directory: {self.root_path}")
            print(f"Output Directory: {output_dir}")
            
            class_folders = self.get_class_folders()
            
            if not class_folders:
                print("\n⚠️  No class folders found!")
                return
            
            print(f"\nFound {len(class_folders)} class(es): {[f.name for f in class_folders]}")
            
            # Process each class
            all_metadata = []
            for class_folder in sorted(class_folders):
                metadata = self.process_class(class_folder, output_dir)
                all_metadata.append(metadata)
                
                # Save merged CSV in the class-specific output folder
                if not metadata['merged_df'].empty and metadata['output_folder']:
                    output_file = metadata['output_folder'] / f"{metadata['class']}_merged.csv"
                    metadata['merged_df'].to_csv(output_file, index=False)
                    print(f"  💾 Saved: {output_file}")
            
            # Print summary
            self.print_summary(all_metadata)
            
        finally:
            # Always stop logging, even if there's an error
            if self.logger:
                self.logger.stop()
    
    def print_summary(self, all_metadata: List[Dict]) -> None:
        """
        Print a summary of all processed classes.
        
        Args:
            all_metadata: List of metadata dictionaries
        """
        print(f"\n{'='*60}")
        print("SUMMARY")
        print(f"{'='*60}")
        
        for meta in all_metadata:
            print(f"\n📊 Class: {meta['class']}")
            print(f"   Files processed: {len(meta['files'])}")
            print(f"   Total rows: {meta['total_rows']:,}")
            print(f"   Total columns: {meta['total_columns']}")
            if meta['output_folder']:
                print(f"   Output folder: {meta['output_folder']}")
            
            if meta['files']:
                print(f"   Individual file details:")
                total_input_rows = 0
                for file_info in meta['files']:
                    ex_label = f"Ex{file_info['ex_number']}" if file_info['ex_number'] else "Unknown"
                    print(f"     • {ex_label}: {file_info['name']} "
                          f"({file_info['rows']:,} rows, {file_info['columns']} cols)")
                    total_input_rows += file_info['rows']
                
                print(f"   Input rows total: {total_input_rows:,}")
                print(f"   Output rows: {meta['total_rows']:,}")