# WinBag2Hiopos

Follow this video to download code repository as a system service in Windows(the program is called nssm)
https://www.youtube.com/watch?v=50-pJN-KJDY 

# Installation guide
Steg 0: 
Ladda ner python(kom ihåg path)
Ladda ner nssm
Ladda ner repository(kodbas) från github
* 

Steg 1:

Öppna nssm kommandotolk

Steg 2: kör följande command line

nssm install "namn_på_system_tjänst" "Path_till_python.exe" "path_till_main.exe"

Steg 3:
Öppna Tjänster på windows, lokalisera den tjänst du skapade, klicka på den, och välj "Starta tjänsten"


Exempel:
"namn_på_system_tjänst": Winbag2Hiopos

"Path_till_python.exe": C:\Users\Kassa1\AppData\Local\Programs\Python\Python313\python.exe

"path_till_main.exe": 
D:\SupportInstallation\HIOPOS_HIOPOS\Utvecklingar\WINBAG2HIOPOS\InstallationSomSystemtjänst\WinBag2Hiopos-main\main.py

D:\SupportInstallation\HIOPOS_HIOPOS\Utvecklingar\WINBAG2HIOPOS\InstallationSomSystemtjänst\WinBag2Hiopos-main\WinBag2Hiopos-main\main.py

Hela command linen:
nssm install "Winbag7Hiopos" "C:\Users\Kassa1\AppData\Local\Programs\Python\Python313\python.exe" "D:\SupportInstallation\HIOPOS_HIOPOS\Utvecklingar\WINBAG2HIOPOS\InstallationSomSystemtjänst\WinBag2Hiopos-main\WinBag2Hiopos-main\main.py"


Paket som behöver installeras
pandas
watchdog
pytz


Kommentar: Om du inte får administration prompt att godkänna så kör cmd som admministratör

cd /d C:\Users

# Columns that are needed in all files for everything to work
Alla kolumner för försäljning filen:
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
* Kod för dokumenttyp
* Dok.Id
* Betalmedel
* Belopp
* Bokföringssuffix


Alla kolumner för följesedlar filen:
* Serie
* Nummer
* Bokföringssuffix
* Dok.Id
* Netto


Alla kolumner för moms filen:
* Butikskod
* Moms
* Basbelopp
* Moms_2
* Totalbelopp

Alla kolumner för presentkort filen:
* Butikskod
* Presentkortskonto
* Kod för kundkortstransaktioner
* Belopp

Alla kolumner för presentkort sålda filen:
* Kort
* Betalmedel
* Belopp
