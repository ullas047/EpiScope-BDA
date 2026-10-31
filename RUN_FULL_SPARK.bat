@echo off
setlocal
cd /d "%~dp0"
set "JAVA_HOME=C:\Program Files\Java\jdk-1.8"
set "PATH=%JAVA_HOME%\bin;%PATH%"
set "EPISCOPE_SPARK_WORKERS=4"
set "EPISCOPE_SPARK_MEMORY=5g"
set "EPISCOPE_HASH_SALT=replace-this-before-sharing-results"
echo Running EpiScope with local PySpark. Initial scale: 0.001
py -3.11 run.py --scale 0.001 --seed 2026
if errorlevel 1 pause
