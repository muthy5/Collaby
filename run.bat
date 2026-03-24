@echo off
echo ================================
echo   Collaby - NYC Listings Scout
echo ================================
echo.

where python >nul 2>nul
if %errorlevel%==0 (
    python scrape.py
    if %errorlevel% neq 0 (
        echo.
        echo Something went wrong. See the error above.
    )
) else (
    where python3 >nul 2>nul
    if %errorlevel%==0 (
        python3 scrape.py
    ) else (
        echo Python is not installed.
        echo.
        echo Download it from: https://www.python.org/downloads/
        echo IMPORTANT: Check the box "Add Python to PATH" during install.
        echo Then close this window and double-click run.bat again.
    )
)

echo.
pause
