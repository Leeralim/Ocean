# Ocean-PRS

## Program for working with PRS-files and unloading hydrological information from PostgreSQL

![image](https://github.com/Leeralim/Ocean/assets/49206103/3bbbd90d-f71b-4ff3-92f7-fab06e8fbca8)


## Description

This program is made for hydrological scientists to work with a database of hydrological information.
After research reises, scientists bring PRS files, which contains various meteorological information, chemical parameters and temperature, sea water salinity. These files need to be processed, checked and entered into the database.


### What is PRS-files?

In essence, this is a regular text file that needs to be parsed:


![image](https://github.com/Leeralim/Ocean/assets/49206103/ee778601-93df-497f-8f68-3bcc5d3573c8)

The header of such a file (outlined by a rectangle of dashes) contains information about the vessel, flight number, station, flight time, coordinates and meteorological information (wind speed, cloudiness, pressure, air temperature), the operator of entering information (i.e. the name of the scientist who produced research).

After entering the data into the database, scientists can make queries on ships and flights and upload the resulting sample to a CSV format for further work with information.


## Functionality

The user can: 
  1. Open PRS-files
  2. Process PRS-files
  3. Check PRS-files for errors (for example, the occurrence of an oxygen (O2) content parameter in a certain range)
  4. Upload the received processing result to CSV format
  5. Load the processed data into the database
  6. Unload data from the database by several parameters (ship, number of reis) and standard sections in the sea (Barents, Norway and etc.).
