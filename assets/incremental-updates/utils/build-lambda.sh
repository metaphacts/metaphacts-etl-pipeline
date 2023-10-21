rm -r tmp
mkdir tmp 
cp -r ../commons tmp/commons
cp ../lambda/$1.py tmp/lambda_function.py
cd tmp
if [ -n "$2" ]; then
    pip install redis -t .
fi
zip -r function.zip .
cp function.zip ../$1.zip
cd ..
rm -r tmp
