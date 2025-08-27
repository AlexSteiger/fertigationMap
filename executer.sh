#!/bin/bash

#change the working directory to the directory of the sh file
cd "$(dirname "$0")"
pwd

#run TTNAdapter
python3 TTNAdapter/TTNAdapter.py

#run currentSMC
cd currentSMC
python3 currentSMC.py
cd ..

#run rainForecastAdapter
cd rainForecastAdapter
python3 openWeatherMapAPI.py
cd ..

#run fertigationMap
cd fertigationMap
python3 fertigationMap.py
cd ../..

