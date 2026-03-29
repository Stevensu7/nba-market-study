@echo off
setlocal
cd /d "%~dp0"
python -m src.main track-day --poll-seconds 300
