@ECHO OFF & SETLOCAL
for %%i in ("%~dp0..") DO SET "folder=%%~fi"
@ECHO ON
..\dist\dwr.exe refresh --days-logs-to-keep 3 --concurrent-procs 3
