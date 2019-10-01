#!/bin/bash	
cp -n ./notebooks/ElectionsByKey/*/*.csv ./Data/Source
rm -dr Postgres
rm -d Data/Processed/*
rm -d Data/SuccessRows/*
rm -d Data/ErrorRows/*
