@echo off
:: For developers: rebuilds dist\WinBag2HioposWatcher.exe from the code in
:: the project root (main.py, import_.py, export.py, logger_setup.py,
:: version.py). Run this after every code change and commit the new .exe,
:: otherwise install.bat installs the old version.
:: Needs Python with PyInstaller:  pip install pyinstaller

cd /d "%~dp0.."
python dev\stamp_version.py
if %errorlevel% neq 0 goto failed

python -m PyInstaller --onefile --name WinBag2HioposWatcher --distpath dist --workpath build --specpath build main.py
if %errorlevel% neq 0 goto failed

echo.
echo Built dist\WinBag2HioposWatcher.exe
pause
exit /b 0

:failed
echo.
echo BUILD FAILED
pause
exit /b 1
