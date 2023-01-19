#!/bin/bash

cd "$(dirname "$0")"

rm -rf .local || true
pybuilderInstalled=`pip freeze | grep 'pybuilder' | wc -l`

if [ $pybuilderInstalled != 1 ]
then
   echo "Installing pybuilder"
   pip install pybuilder --pre
fi
export PATH=$PATH:$(pwd)/.local/bin

pyb

if [ ! -d "bin" ]; then
  mkdir 'bin'
fi

cp target/dist/diksha-dataproducts-adhoc-jobs*/dist/* bin/
mv bin/diksha-dataproducts-adhoc-jobs-*.tar.gz bin/diksha-dataproducts-adhoc-jobs.tar.gz
mv bin/diksha_dataproducts_adhoc_jobs-*.whl bin/diksha-dataproducts-adhoc-jobs.whl

rm -rf target