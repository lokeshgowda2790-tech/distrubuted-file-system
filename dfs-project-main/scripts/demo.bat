@echo off
REM demo.bat — Demo script for Mini DFS (Windows CMD)
REM Usage: run from project root in cmd.exe

set PROJ=%CD%

echo === Demo starting in project: %PROJ% ===

REM --- clean / prepare folders ---
rmdir /s /q "%PROJ%\data" 2>nul
rmdir /s /q "%PROJ%\logs" 2>nul

mkdir "%PROJ%\data" 2>nul
mkdir "%PROJ%\data\sn1" 2>nul
mkdir "%PROJ%\data\sn2" 2>nul
mkdir "%PROJ%\data\input" 2>nul
mkdir "%PROJ%\data\output" 2>nul
mkdir "%PROJ%\logs" 2>nul

echo Hello Distributed World > "%PROJ%\data\input\test.txt"

REM --- build ---
echo Building project (mvn clean package)...
mvn clean package
if errorlevel 1 (
  echo Maven build failed. Aborting demo.
  exit /b 1
)

REM --- start Master in new window ---
start "Master" cmd /k "cd /d %PROJ% && java -cp target\dfs-project-1.0-SNAPSHOT.jar master.MasterServer 9000 2 16 > \"%PROJ%\logs\master.log\" 2>&1"
echo Master started (logs -> logs\master.log)
timeout /t 1 >nul

REM --- start StorageNode 1 in new window ---
start "SN1" cmd /k "cd /d %PROJ% && java -cp target\dfs-project-1.0-SNAPSHOT.jar storagenode.StorageNode 127.0.0.1 9000 10001 \"%PROJ%\data\sn1\" > \"%PROJ%\logs\sn1.log\" 2>&1"
echo StorageNode1 started (logs -> logs\sn1.log)
timeout /t 1 >nul

REM --- start StorageNode 2 in new window ---
start "SN2" cmd /k "cd /d %PROJ% && java -cp target\dfs-project-1.0-SNAPSHOT.jar storagenode.StorageNode 127.0.0.1 9000 10002 \"%PROJ%\data\sn2\" > \"%PROJ%\logs\sn2.log\" 2>&1"
echo StorageNode2 started (logs -> logs\sn2.log)
timeout /t 3 >nul

REM --- run client put ---
echo Uploading file via client...
java -cp target\dfs-project-1.0-SNAPSHOT.jar client.DFSClient 127.0.0.1 9000 put "%PROJ%\data\input\test.txt"

REM --- run client get ---
echo Downloading file via client...
java -cp target\dfs-project-1.0-SNAPSHOT.jar client.DFSClient 127.0.0.1 9000 get test.txt "%PROJ%\data\output\file_out.txt"

REM --- verification ---
echo.
echo --- Verification ---
echo Files in data\sn1:
dir "%PROJ%\data\sn1"

echo.
echo Files in data\sn2:
dir "%PROJ%\data\sn2"

for /f "delims=" %%f in ('dir /b "%PROJ%\data\sn1"') do set CHUNK=%%f & goto :afterchunk
:afterchunk

if defined CHUNK (
  echo Sample chunk name: %CHUNK%
  echo Checksum sn1:
  certutil -hashfile "%PROJ%\data\sn1\%CHUNK%" SHA256
  echo.
  echo Checksum sn2:
  certutil -hashfile "%PROJ%\data\sn2\%CHUNK%" SHA256
) else (
  echo No chunks found in sn1 (something went wrong).
)

echo.
echo Original file checksum:
certutil -hashfile "%PROJ%\data\input\test.txt" SHA256

echo.
echo Fetched file checksum:
certutil -hashfile "%PROJ%\data\output\file_out.txt" SHA256

echo.
echo Demo finished. Check logs\ and data\ folders.
pause
