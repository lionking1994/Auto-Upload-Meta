# PowerShell script to activate virtual environment
Write-Host "Activating Python virtual environment..." -ForegroundColor Green
& ".\venv\Scripts\Activate.ps1"
Write-Host "Virtual environment activated!" -ForegroundColor Green
Write-Host ""
Write-Host "To run the scripts:" -ForegroundColor Yellow
Write-Host "  - Test Snowflake: python test_snowflake.py"
Write-Host "  - Dry run: python main_with_snowflake.py --dry-run --top-n 5"
Write-Host "  - Upload: python main_with_snowflake.py --top-n 100 --ad-account-id act_YOUR_ID"
Write-Host ""
