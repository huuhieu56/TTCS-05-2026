# Update pipeline script
Set-Location (Split-Path $PSScriptRoot -Parent)
python -m pipelines.run_pipeline @args
