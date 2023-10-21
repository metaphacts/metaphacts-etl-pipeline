source ./secrets.sh
export PYTHONPATH=$(pwd)
source ~/venv/bin/activate
python ec2-scripts/incremental-updates-ingestion.py
