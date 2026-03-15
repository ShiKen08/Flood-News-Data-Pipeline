#!/usr/bin/env python3
"""
setup.py  ·  Flood Data Pipeline — One-time setup script

Run this after cloning the repo:
    python setup.py

What it does:
  1. Creates all required local directories (cache/, output/, logs/, etc.)
  2. Copies config.py.template → config.py if config.py doesn't exist yet
  3. Checks that required Python packages are installed
  4. Prints next steps
"""

import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent

# Directories that must exist locally but are NOT committed to git
REQUIRED_DIRS = [
    ROOT / "cache",
    ROOT / "output",
    ROOT / "logs",
    ROOT / "raw_index_responses",
]

REQUIRED_PACKAGES = [
    "pandas",
    "pyarrow",
    "requests",
    "trafilatura",
    "bs4",
    "langdetect",
    "tqdm",
]

def create_directories():
    print("📁 Creating local directories...")
    for d in REQUIRED_DIRS:
        d.mkdir(parents=True, exist_ok=True)
        # Add a .gitkeep so the dir structure is clear but contents aren't tracked
        gitkeep = d / ".gitkeep"
        if not gitkeep.exists():
            gitkeep.touch()
        print(f"   ✓ {d.relative_to(ROOT)}/")


def setup_config():
    config_path     = ROOT / "config.py"
    template_path   = ROOT / "config.py.template"

    if config_path.exists():
        print("\n⚙️  config.py already exists — skipping copy.")
    else:
        shutil.copy(template_path, config_path)
        print("\n⚙️  Copied config.py.template → config.py")
        print("   ⚠️  Open config.py and fill in your local paths before running the pipeline.")


def check_packages():
    print("\n📦 Checking Python packages...")
    missing = []
    for pkg in REQUIRED_PACKAGES:
        try:
            __import__(pkg)
            print(f"   ✓ {pkg}")
        except ImportError:
            print(f"   ✗ {pkg}  ← MISSING")
            missing.append(pkg)

    if missing:
        print(f"\n   Install missing packages with:")
        print(f"   pip install -r requirements.txt")
        return False
    return True


def print_next_steps(packages_ok):
    print("\n" + "=" * 60)
    print("  Setup complete. Next steps:")
    print("=" * 60)

    if not packages_ok:
        print("  1. pip install -r requirements.txt")
        step = 2
    else:
        step = 1

    print(f"  {step}. Open config.py and set your local paths")
    print(f"  {step+1}. Check progress/ to see which flood events are already done")
    print(f"  {step+2}. Run the pipeline starting from the first incomplete stage")
    print(f"\n  See README.md for the full walkthrough.\n")


if __name__ == "__main__":
    print("\n🌊 Flood Pipeline — Setup\n")
    create_directories()
    setup_config()
    packages_ok = check_packages()
    print_next_steps(packages_ok)
