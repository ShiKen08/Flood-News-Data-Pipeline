#!/usr/bin/env python3
"""
setup.py  ·  Flood Data Pipeline — One-time setup script

Run this after cloning the repo:
    python3 setup.py

What it does:
  1. Creates all required local directories (cache/, output/, logs/, etc.)
  2. Copies config.py.template → config.py if config.py doesn't exist yet
  3. Checks that required Python packages are installed
  4. Offers to install missing packages using the same Python that ran this script
  5. Prints next steps
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

# (import_name, pip_package_name) — import name is used for __import__ check
REQUIRED_PACKAGES = [
    ("pandas",       "pandas"),
    ("pyarrow",      "pyarrow"),
    ("numpy",        "numpy"),
    ("requests",     "requests"),
    ("urllib3",      "urllib3"),
    ("trafilatura",  "trafilatura"),
    ("bs4",          "beautifulsoup4"),
    ("lxml",         "lxml"),
    ("chardet",      "chardet"),
    ("langid",       "langid"),
    ("tqdm",         "tqdm"),
    ("pytest",       "pytest"),
    ("aiohttp",      "aiohttp"),
]


def create_directories():
    print("Creating local directories...")
    for d in REQUIRED_DIRS:
        d.mkdir(parents=True, exist_ok=True)
        gitkeep = d / ".gitkeep"
        if not gitkeep.exists():
            gitkeep.touch()
        print(f"   ✓ {d.relative_to(ROOT)}/")


def setup_config():
    config_path   = ROOT / "config.py"
    template_path = ROOT / "config.py.template"

    if config_path.exists():
        print("\nconfig.py already exists — skipping copy.")
    else:
        shutil.copy(template_path, config_path)
        print("\nCopied config.py.template -> config.py")
        print("   Open config.py and review settings before running the pipeline.")


def check_packages():
    print("\nChecking Python packages...")
    missing = []
    for import_name, pip_name in REQUIRED_PACKAGES:
        try:
            __import__(import_name)
            print(f"   ✓ {pip_name}")
        except ImportError:
            print(f"   ✗ {pip_name}  <- MISSING")
            missing.append(pip_name)
    return missing


def install_packages(missing: list[str]) -> bool:
    """Install missing packages using the same Python interpreter running this script."""
    req_file = ROOT / "requirements.txt"
    print(f"\n   Installing via: {sys.executable} -m pip install -r requirements.txt")
    result = subprocess.run(
        [sys.executable, "-m", "pip", "install", "-r", str(req_file)],
        capture_output=False,
    )
    if result.returncode != 0:
        print("\n   pip install failed. Try manually:")
        print(f"   {sys.executable} -m pip install -r requirements.txt")
        print("\n   On shared/HPC systems you may need --user:")
        print(f"   {sys.executable} -m pip install --user -r requirements.txt")
        return False
    return True


def print_next_steps(packages_ok: bool):
    print("\n" + "=" * 60)
    print("  Setup complete. Next steps:")
    print("=" * 60)

    step = 1
    if not packages_ok:
        print(f"  {step}. Some packages still missing — see errors above")
        print(f"     Try: {sys.executable} -m pip install --user -r requirements.txt")
        step += 1

    print(f"  {step}. Open config.py and review PILOT_FLOOD_IDS and paths")
    print(f"  {step+1}. Run: python3 stage_00_preflight.py")
    print(f"  {step+2}. Then follow the stage sequence in README.md")
    print(f"\n  Python: {sys.executable}")
    print(f"  Version: {sys.version.split()[0]}\n")


if __name__ == "__main__":
    print("\nFlood Pipeline — Setup\n")
    create_directories()
    setup_config()

    missing = check_packages()

    if missing:
        print(f"\n   {len(missing)} package(s) missing: {', '.join(missing)}")
        answer = input("   Install now? [Y/n] ").strip().lower()
        if answer in ("", "y", "yes"):
            ok = install_packages(missing)
            if ok:
                # Re-check after install
                print("\nRe-checking packages...")
                missing = check_packages()
        packages_ok = len(missing) == 0
    else:
        packages_ok = True

    print_next_steps(packages_ok)
