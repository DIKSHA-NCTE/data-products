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

cp target/dist/diksha-dataproducts-scheduled-jobs*/dist/* bin/
mv bin/diksha-dataproducts-scheduled-jobs-*.tar.gz bin/diksha-dataproducts-scheduled-jobs.tar.gz
mv bin/diksha_dataproducts_scheduled_jobs-*.whl bin/diksha-dataproducts-scheduled-jobs.whl

rm -rf target