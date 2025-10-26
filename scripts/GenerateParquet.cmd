@echo off
setlocal

set SCRIPT_NAME=GenerateParquetTestData.py
set GENERATED_FILE=TEST_DATA.parquet
set DEST_DIR=..\out\build\x64-Debug

echo Running Python script: %SCRIPT_NAME%
python %SCRIPT_NAME%

echo.
echo Copying %GENERATED_FILE% to %DEST_DIR%
copy %GENERATED_FILE% %DEST_DIR%

echo.
echo Done.
pause

endlocal
