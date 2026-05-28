# run_cutter.ps1 -- wrapper for csv_cutter.py that clears conflicting Python env vars
# Usage: .\run_cutter.ps1 <csv_file> [pattern1 pattern2 ...]
#   Omit patterns to browse all columns interactively.
param([Parameter(Mandatory, Position=0)] [string]$CsvFile, [Parameter(ValueFromRemainingArguments)] [string[]]$Patterns)

$env:PYTHONPATH = ''
$env:PYTHONHOME = ''
& "C:\Users\FrancisBarchesky\AppData\Local\Programs\Python\Python313\python.exe" `
    "C:\Users\FrancisBarchesky\Documents\GitHub\flight-test-analyzer\csv_cutter.py" `
    $CsvFile @Patterns
