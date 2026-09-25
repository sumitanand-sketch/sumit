@echo off
echo Starting Vault Distributed Object Storage Cluster...
echo Web Dashboard: http://127.0.0.1:8000/dashboard
echo S3 API Endpoint: http://127.0.0.1:8000
python -m vault.cli serve --port 8000 --nodes 3
pause
