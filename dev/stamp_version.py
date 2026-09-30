"""Writes the current time into version.py. Run by build.bat right before
PyInstaller runs, so the timestamp baked into the .exe is the actual build
time - not, say, whenever the code was last committed."""
import os
from datetime import datetime


def main():
    ts = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %z")
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    version_path = os.path.join(project_root, "version.py")
    with open(version_path, "w") as f:
        f.write(f'BUILD_TIMESTAMP = "{ts}"\n')
    print(f"Stamped version.py: {ts}")


if __name__ == "__main__":
    main()
