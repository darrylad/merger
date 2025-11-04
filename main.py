"""
Main entry point for the CSV merger application.
"""

import sys
from merger import CSVMerger


def main():
    """Main function to run the CSV merger."""
    
    # Check command line arguments
    if len(sys.argv) < 2:
        print("Usage: python main.py <root_directory_path> [output_directory]")
        print("\nExample:")
        print("  python main.py /Users/darrylad/Darryl/Research/Darryl/Data")
        print("  python main.py /path/to/data /path/to/output")
        sys.exit(1)
    
    root_path = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else None
    
    try:
        # Create merger and run
        merger = CSVMerger(root_path)
        merger.run(output_dir)
        
        print("\n✅ Processing complete!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()