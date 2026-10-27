# scripts\demo.ps1 — Demo script for Mini DFS (PowerShell)
# Starts Master + 3 storage nodes, shows live output and saves logs with Tee-Object.
# Usage (from project root):
#   Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
#   .\scripts\demo.ps1

$proj = (Get-Location).Path
Write-Host "=== Demo starting in project: $proj ==="

# --- prepare folders ---
Remove-Item -Recurse -Force "$proj\data" -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force "$proj\logs" -ErrorAction SilentlyContinue

New-Item -ItemType Directory -Path "$proj\data\sn1" -Force | Out-Null
New-Item -ItemType Directory -Path "$proj\data\sn2" -Force | Out-Null
New-Item -ItemType Directory -Path "$proj\data\sn3" -Force | Out-Null
New-Item -ItemType Directory -Path "$proj\data\input" -Force | Out-Null
New-Item -ItemType Directory -Path "$proj\data\output" -Force | Out-Null
New-Item -ItemType Directory -Path "$proj\logs" -Force | Out-Null

# create test file
"Hello Distributed Replication - $(Get-Date -Format o)" | Out-File -FilePath "$proj\data\input\test.txt" -Encoding utf8

# --- build ---
Write-Host "`n==> Building project (mvn clean package). This may take a moment..."
$mvn = "mvn"
$proc = Start-Process -FilePath $mvn -ArgumentList "clean","package" -WorkingDirectory $proj -NoNewWindow -PassThru -Wait
if ($proc.ExitCode -ne 0) {
    Write-Host "Maven build failed (exit $($proc.ExitCode)). Aborting demo." -ForegroundColor Red
    exit 1
}
Write-Host "Build finished."

Start-Sleep -Seconds 1

# helper to start a Java process in a new window with Tee-Object
function Start-LoggedProcess($title, $cmd, $logPath) {
    $pscmd = @"
Write-Host '=== $title ===';
Set-Location -Path '$proj';
$cmd | Tee-Object -FilePath '$logPath';
"@
    Start-Process -FilePath "powershell" -ArgumentList "-NoExit","-Title",$title,"-Command",$pscmd
    Write-Host "$title started (logs -> $logPath)"
}

# --- start Master in new window with Tee-Object ---
$masterLog = "$proj\logs\master.log"
$masterCmd = "java -cp target\dfs-project-1.0-SNAPSHOT.jar master.MasterServer 9000 2 16"
Start-LoggedProcess "MASTER" $masterCmd $masterLog
Start-Sleep -Seconds 1

# --- start StorageNode 1 ---
$sn1Log = "$proj\logs\sn1.log"
$sn1Cmd = "java -cp target\dfs-project-1.0-SNAPSHOT.jar storagenode.StorageNode 127.0.0.1 9000 10001 `"$proj\data\sn1`""
Start-LoggedProcess "SN1" $sn1Cmd $sn1Log
Start-Sleep -Seconds 1

# --- start StorageNode 2 ---
$sn2Log = "$proj\logs\sn2.log"
$sn2Cmd = "java -cp target\dfs-project-1.0-SNAPSHOT.jar storagenode.StorageNode 127.0.0.1 9000 10002 `"$proj\data\sn2`""
Start-LoggedProcess "SN2" $sn2Cmd $sn2Log
Start-Sleep -Seconds 1

# --- start StorageNode 3 (spare for re-replication) ---
$sn3Log = "$proj\logs\sn3.log"
$sn3Cmd = "java -cp target\dfs-project-1.0-SNAPSHOT.jar storagenode.StorageNode 127.0.0.1 9000 10003 `"$proj\data\sn3`""
Start-LoggedProcess "SN3" $sn3Cmd $sn3Log
Start-Sleep -Seconds 3

# --- run client put ---
Write-Host "`n==> Uploading file via client..."
& java -cp "target\dfs-project-1.0-SNAPSHOT.jar" client.DFSClient 127.0.0.1 9000 put "$proj\data\input\test.txt"

Start-Sleep -Seconds 1

# --- run client get ---
Write-Host "`n==> Downloading file via client..."
& java -cp "target\dfs-project-1.0-SNAPSHOT.jar" client.DFSClient 127.0.0.1 9000 get test.txt "$proj\data\output\file_out.txt"

# --- verification ---
Write-Host "`n--- Verification ---"

Write-Host "`nFiles in data\sn1:"
Get-ChildItem -Path "$proj\data\sn1" | Select Name, Length

Write-Host "`nFiles in data\sn2:"
Get-ChildItem -Path "$proj\data\sn2" | Select Name, Length

Write-Host "`nFiles in data\sn3:"
Get-ChildItem -Path "$proj\data\sn3" | Select Name, Length

$chunk = (Get-ChildItem -Path "$proj\data\sn1" | Select-Object -First 1).Name
if ($null -ne $chunk) {
    Write-Host "`nSample chunk name: $chunk"
    Write-Host "`nChecksum sn1:"
    Get-FileHash -Path "$proj\data\sn1\$chunk" -Algorithm SHA256 | Format-List

    Write-Host "`nChecksum sn2:"
    Get-FileHash -Path "$proj\data\sn2\$chunk" -Algorithm SHA256 | Format-List

    Write-Host "`nChecksum sn3 (if present):"
    if (Test-Path "$proj\data\sn3\$chunk") { Get-FileHash -Path "$proj\data\sn3\$chunk" -Algorithm SHA256 | Format-List } else { Write-Host "  chunk not present on sn3 yet" }
} else {
    Write-Host "`nNo chunks found in sn1 (something went wrong)."
}

Write-Host "`nOriginal file checksum:"
Get-FileHash -Path "$proj\data\input\test.txt" -Algorithm SHA256

Write-Host "`nFetched file checksum:"
Get-FileHash -Path "$proj\data\output\file_out.txt" -Algorithm SHA256

Write-Host "`nDemo finished. Logs -> $proj\logs\\"
Write-Host "To test re-replication: close the PowerShell window for StorageNode2 (SN2) or kill its process; then watch the MASTER window to observe re-replication."
