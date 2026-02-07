import sys
import os
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.ui.app import main as run_app

def main():
    """Entry point for the Weather Data Analyzer application."""
    print("🌤️ Weather Data Analyzer")
    print("=" * 50)
    run_app()

if __name__ == "__main__":
    main()
