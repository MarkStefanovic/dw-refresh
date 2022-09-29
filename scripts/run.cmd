@ECHO OFF & SETLOCAL
for %%i in ("%~dp0..") DO SET "folder=%%~fi"
@ECHO ON
conda run -n dw-refresh --cwd %folder% --live-stream python -m src.main refresh --days-logs-to-keep 3
