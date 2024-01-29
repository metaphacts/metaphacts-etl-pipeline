#!/bin/sh

SCRIPT_DIR="$(dirname $0)"

DATASET_URL="https://dimensions.figshare.com/ndownloader/files/30718538"
DATASET_DIR="source-files/covid"
EXTRACT_DIR="$DATASET_DIR/tmp"
DATASET_FILE="$EXTRACT_DIR/dimensions-covid19-export.zip"

RDF_FOLDER="$SCRIPT_DIR/source-files"

LINES_PER_FILE=5000

# cd to project root directoy
cd "$SCRIPT_DIR/../.."

mkdir -p "$DATASET_DIR"
mkdir -p "$EXTRACT_DIR"

if [ ! -f "$DATASET_FILE" ]; then
    echo "downloading example dataset from '$DATASET_URL' to '$DATASET_FILE'"
    curl --location --output "$DATASET_FILE" "$DATASET_URL"
else 
    echo "example dataset from '$DATASET_URL' is already available at '$DATASET_FILE'"
fi

echo "extracting archive file"
unzip -u -j -n "$DATASET_FILE" -d "$EXTRACT_DIR"

echo; echo

echo "original dataset files:"
ls -l "$EXTRACT_DIR"

echo; echo

echo "removing existing CSV files"
rm $DATASET_DIR/*.csv

echo; echo

echo "splitting CSV files into max $LINES_PER_FILE lines per file"
csvfiles=$(find "$EXTRACT_DIR" -name "*.csv")
for csvfile in $csvfiles; do
    echo "splitting CSV file '$csvfile'"
    partname=$(basename $csvfile .csv)
    python3 "$SCRIPT_DIR/split-csv.py" "$csvfile" "$DATASET_DIR" "$partname" "$LINES_PER_FILE"
done

echo; echo

echo "copying additional source files"
cp $RDF_FOLDER/* "$DATASET_DIR"

echo; echo

echo "final source files:"
ls -l "$DATASET_DIR"

echo; echo

echo "upload all source files to S3 for use in the ETL pipeline using the following command:"
echo "(make sure to use the proper target S3 bucket for source files)"
bucketname=$(aws s3api list-buckets --query "Buckets[].[Name]" --output text|grep etlpipelinestack|grep sourcebucket)
echo "aws s3 cp $DATASET_DIR/ s3://$bucketname --recursive --include \"*\" --exclude \"tmp/*\""