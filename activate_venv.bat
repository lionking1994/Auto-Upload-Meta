@echo off
echo Activating Python virtual environment...
call venv\Scripts\activate.bat
echo Virtual environment activated!
echo.
echo To run the scripts:
echo   - Test Snowflake: python test_snowflake.py
echo   - Dry run: python main_with_snowflake.py --dry-run --top-n 5
echo   - Upload: python main_with_snowflake.py --top-n 100 --ad-account-id act_YOUR_ID
echo.
