#!/bin/bash
mkdir ./Data
mkdir ./Data/Source
mkdir ./Data/ErrorRows
mkdir ./Data/Processed
mkdir ./Data/SuccessRows
cp -n ./notebooks/ElectionsByKey/*/*.csv ./Data/Source
rm -f -r Postgres
rm -f -r Data/Processed/*
rm -f -r Data/SuccessRows/*
rm -f -r Data/ErrorRows/*
chmod 777 ./Data
chmod 777 ./Data/Source

