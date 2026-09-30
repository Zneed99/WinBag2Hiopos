# WinBag2Hiopos

Watches a folder for files exported by WinBag, converts them, and produces
the files HioPOS needs - both directions:

  - **Export**: Försäljning / Betalsätt / Följesedlar / Moms (+ optional
    Presentkort files) dropped in `Input_Files_Here` are combined into
    per-store `.TXT` files for HioPOS.
  - **Import**: a `pcs.adm` file dropped in the data folder is split into
    `file_01_11`, `file_artiklar`, `file_huvudgrupp` and `file_varugrupp`
    for HioPOS to import (customers, articles incl. barcode/ingredients,
    main groups, sub groups).

Runs as a Windows service (via NSSM) and writes everything it does to a log
file, so a failed run can be diagnosed after the fact - see TROUBLESHOOTING.


PROJECT STRUCTURE
------------------
    README.md
    main.py               the service: watches the data folder, routes to
                           export/import, archives processed files
    export.py              builds the HioPOS export files
    import_.py              splits pcs.adm into the 4 HioPOS import files
    logger_setup.py         the log file
    version.py              build timestamp, shown at startup (see below)
    install\              everything needed to install at a customer
      install.bat            double-click to install / update the service
      uninstall.bat          double-click to remove the service
      install.ps1            (used by install.bat)
      uninstall.ps1          (used by uninstall.bat)
      nssm.exe               the tool that runs the program as a service
    dev\
      build.bat              rebuilds dist\WinBag2HioposWatcher.exe after
                              code changes
    dist\                  ready-built program (no Python needed to run it)
      WinBag2HioposWatcher.exe


HOW IT WORKS
------------
STANDARD DATA FOLDER - fixed, the same on every installation:

    C:\winbag_export\
      Input_Files_Here\      <- drop the export files here (Försäljning,
                               Betalsätt, Följesedlar, Moms, Presentkort)
      (root)                 <- drop pcs.adm here (any case: PCS.ADM,
                               pcs.adm, ... are all detected)
      PCS_Archive\            <- the original pcs.adm, after import
      Old Files\              <- the original export files, after export
      Imported_Files\         <- file_01_11 / file_artiklar / file_huvudgrupp
                               / file_varugrupp for HioPOS to pick up
      Logs\
        winbag2hiopos.log     <- everything the program did (look here first)

    Windows service:  ICG - WinBag2Hiopos   (service name ICG-WinBag2Hiopos)
    Program install location (the .exe itself, not the data): C:\ICG\WinBag2Hiopos\App

- A file is only acted on once it has stopped growing (WinBag can still be
  writing it when it first appears), so a slow write does not cause partial
  or corrupted output.
- The original pcs.adm / export files are archived *before* processing
  starts, so an unrelated file event can never cause the same file to be
  re-imported or re-exported.
- Import output files are written under a temporary name and only renamed
  to their final name once the whole pcs.adm has been processed without
  error, so an interrupted run never leaves a partial file under a name
  that looks complete.


=====================================================================
QUICK INSTALL AT A CUSTOMER (recommended)
=====================================================================
The customer's computer needs NOTHING installed (no Python, no git).
Everything needed is in this project: `dist\WinBag2HioposWatcher.exe` and
the `install\` folder.

1. On the customer's computer, open the repo on GitHub and click
   Code -> Download ZIP. (The repo is private, so you must be logged in
   with an account that has access.)
   Right-click the ZIP -> Properties -> tick "Unblock" (if shown) -> OK,
   then right-click -> Extract All. Any folder is fine, spaces are OK.
   (`git clone` works too if git is installed.)

2. Open the `install` folder in the project and double-click `install.bat`.
   Click "Yes" when Windows asks for administrator rights.
   (If Windows shows "Windows protected your PC": More info -> Run anyway.)

3. The window shows "ICG - WinBag2Hiopos is installed and running" and
   where the folders are. Press a key to close it.

4. Test: drop an export file set (or a pcs.adm) into the relevant folder
   under `C:\winbag_export` and check `C:\winbag_export\Logs\winbag2hiopos.log`
   for activity within a few seconds.

What install.bat does:
  - copies WinBag2HioposWatcher.exe and nssm.exe to `C:\ICG\WinBag2Hiopos\App\`
  - installs the Windows service "ICG - WinBag2Hiopos" (starts with Windows,
    restarts automatically if it crashes) and starts it
  - does **not** touch `C:\winbag_export` (the data folder) beyond what the
    program itself already creates there when it runs

UPDATE to a new version: get the new project version onto the computer and
double-click `install\install.bat` again. The old service is replaced.

UNINSTALL: double-click `install\uninstall.bat`. The service is removed;
`C:\ICG\WinBag2Hiopos` (the program) and `C:\winbag_export` (the data,
files and logs) are both kept.

!!! FOR DEVELOPERS: after changing the code, double-click `dev\build.bat`
!!! to rebuild `dist\WinBag2HioposWatcher.exe` and commit it -
!!! install.bat installs whatever .exe is in `dist\`.


=====================================================================
EVERYDAY COMMANDS (command prompt as administrator, in C:\ICG\WinBag2Hiopos\App)
=====================================================================
    nssm status ICG-WinBag2Hiopos     -> is it running?
    nssm stop ICG-WinBag2Hiopos       -> stop
    nssm start ICG-WinBag2Hiopos      -> start
    nssm restart ICG-WinBag2Hiopos    -> restart
    nssm edit ICG-WinBag2Hiopos       -> open the NSSM settings window

UNINSTALL (without uninstall.bat)
    nssm stop ICG-WinBag2Hiopos
    nssm remove ICG-WinBag2Hiopos confirm


=====================================================================
TROUBLESHOOTING
=====================================================================
WHERE TO LOOK (same on every installation):
  1. C:\winbag_export\Logs\winbag2hiopos.log  - what happened to each file,
     with a timestamp, module and level (INFO/WARNING/ERROR) on every line.
     The very first line after a restart shows the build timestamp
     (from version.py) so you can confirm which version is actually running.
  2. C:\ICG\WinBag2Hiopos\Logs\service_output.log - if the service crashes
     or will not start (startup errors land here, not in winbag2hiopos.log)
  3. C:\ICG\WinBag2Hiopos\Logs\install.log        - if install.bat failed
  4. Services (Tjänster) -> "ICG - WinBag2Hiopos" - is it running?

- pcs.adm is not being picked up:
  It must be dropped directly in `C:\winbag_export` (not `Input_Files_Here`,
  that folder is for the export-side files only). Filename case does not
  matter (`pcs.adm`, `PCS.ADM`, ... are all detected).

- A run looks incomplete / rows are missing:
  Check `winbag2hiopos.log` for the "Rows written: {...}" summary line for
  that run, and any WARNING lines just above it (skipped/malformed rows,
  unmatched butikskod, price parsing issues, etc.) - these say exactly what
  was skipped and why, rather than failing silently.

- The service does not start:
  Look in `C:\ICG\WinBag2Hiopos\Logs\service_output.log`.
  "nssm edit ICG-WinBag2Hiopos" shows the current NSSM settings.


=====================================================================
FOR DEVELOPERS
=====================================================================
Run from source (needs Python 3 with `pip install pandas watchdog pytz`):
    python main.py

Build the .exe into dist\ (after every code change, then commit):
    dev\build.bat

Where things are in the code:
  - Export logic:                    export.py
  - Import (pcs.adm splitting):      import_.py  import_action()
  - Row transforms (per record type): import_.py  transform_01_11(),
                                       transform_02_22(), transform_huvudgrupp(),
                                       transform_varugrupp()
  - Watching/routing/archiving:       main.py
  - The log file:                     logger_setup.py

Install the service with Python instead of the .exe (Python must then be
installed on that computer, with its path added to the SYSTEM variables):
    nssm install ICG-WinBag2Hiopos "C:\Path\To\python.exe" "C:\Path\To\project\main.py"
    nssm set ICG-WinBag2Hiopos AppDirectory "C:\Path\To\project"


# Columns that are needed in all files for everything to work
Alla kolumner för försäljning filen:
* Serie
* Butikskod
* ButikskodWinbag
* KassaId
* Dok.datum
* Serie
* Referens
* Enh.1
* Pris 
* Timme
* Anställd
* Moms
* Kod för dokumenttyp
* Netto
* Varugruppskod


Alla kolumner för betalsätt filen:
* Serie
* Nummer
* ButikskodWinbag
* Kod för dokumenttyp
* Dok.Id
* Betalmedel
* Belopp
* Bokföringssuffix


Alla kolumner för följesedlar filen:
* Serie
* ButikskodWinbag
* Nummer
* Bokföringssuffix
* Dok.Id
* Netto


Alla kolumner för moms filen:
* Butikskod
* ButikskodWinbag
* Moms
* Basbelopp
* Moms_2
* Totalbelopp

Alla kolumner för presentkort used filen:
* Butikskod
* Presentkortskonto
* Kod för kundkortstransaktioner
* Belopp

Alla kolumner för presentkort sålda filen:
* Kort
* Betalmedel
* Belopp
