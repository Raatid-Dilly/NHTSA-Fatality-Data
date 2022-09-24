#!/bin/bash

mkdir  -p ./accident ./person ./vehicle
for year in {1975..2020}
do
	mkdir car_$year && cd car_$year
	wget -q -O car_$year.zip https://www.nhtsa.gov/file-downloads/download?p=nhtsa/downloads/FARS/${year}/National/FARS${year}NationalCSV.zip && unzip -q car_$year.zip && rm car_$year.zip
	mv ACCIDENT.CSV ../accident/${year}_accident.csv
	mv PERSON.CSV ../person/${year}_person.csv
	mv VEHICLE.CSV ../vehicle/${year}_vehicle.csv
	cd ..
	rm  -r -d car_$year
done
