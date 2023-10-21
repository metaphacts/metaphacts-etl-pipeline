source ./secrets.sh
export PYTHONPATH=$(pwd)
source ~/venv/bin/activate
python ec2-scripts/sparql-postprocessing.py
