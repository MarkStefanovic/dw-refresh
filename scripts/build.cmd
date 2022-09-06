@ECHO OFF & SETLOCAL
for %%i in ("%~dp0..") DO SET "folder=%%~fi"
@ECHO ON
del /S /Q "%folder%\dist\"
ECHO D | xcopy /Y /E "%folder%\assets" "%folder%\dist\assets"
conda run -n dw-refresh pyinstaller "%folder%\app.spec" --distpath %folder%\dist\ --workpath %folder%\build
