@echo off
setlocal

set "PYTHON_EXE=C:\Program Files\QGIS 3.40.7\apps\Python312\python.exe"
set "PROXY_URL=http://proxy-bvcol.admin.ch:8080"
set "REQ_FILE=%~dp0requirements-docs.txt"

if not exist "%PYTHON_EXE%" (
  echo [ERROR] Python executable not found: %PYTHON_EXE%
  exit /b 1
)

if not exist "%REQ_FILE%" (
  echo [ERROR] requirements-docs.txt not found: %REQ_FILE%
  exit /b 1
)

"%PYTHON_EXE%" -m pip --proxy %PROXY_URL% --trusted-host pypi.org --trusted-host files.pythonhosted.org install -r "%REQ_FILE%"
exit /b %ERRORLEVEL%
