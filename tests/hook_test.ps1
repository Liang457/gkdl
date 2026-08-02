param([string]$file)
Write-Output "HOOK_RUN file=$file"
Write-Output "URL=$env:GKDL_URL"
Write-Output "GID=$env:GKDL_GID"
Write-Output "SIZE=$env:GKDL_SIZE"
Write-Output "SHA=$env:GKDL_SHA256"
Write-Error "stderr line test"
exit 0
