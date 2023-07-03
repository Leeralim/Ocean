# Ocean-PRS

## Program for working with PRS-files and unloading hydrological information from PostgreSQL

![image](https://github.com/Leeralim/Ocean/assets/49206103/3bbbd90d-f71b-4ff3-92f7-fab06e8fbca8)


## Description

This program is made for hydrological scientists to work with a database of hydrological information.
After research reises, scientists bring _PRS files_, which contains various meteorological information, chemical parameters and temperature, sea water salinity. These files need to be processed, checked and entered into the database.


### What is PRS-files?

In essence, this is a regular text file that needs to be parsed:


![image](https://github.com/Leeralim/Ocean/assets/49206103/7ebfb5e2-f2d4-4178-b4e4-dfaee1f79ac9)


* The header of such a file (outlined by a rectangle of dashes) contains information about the vessel, flight number, station, flight time, coordinates and meteorological information (wind speed, cloudiness, pressure, air temperature), the operator of entering information (i.e. the name of the scientist who produced research).
* Below, in a columnar form, there is information about temperature, water salinity, observation horizon and chemical parameters of water (they are not in all PRS files, but this is also taken into account during processing)

After entering the data into the database, scientists can make queries on ships and flights and upload the resulting sample to a CSV format for further work with information.



## Functionality

### The user can: 
  1. Open PRS-files
  2. Process PRS-files
  3. Check PRS-files for errors (for example, the occurrence of an oxygen (O2) content parameter in a certain range)
  4. Upload the received processing result to CSV format
  5. Load the processed data into the database
  6. Unload data from the database by several parameters (ship, number of reis) and standard sections in the sea (Barents, Norway and etc.).

## Stack

### Development language - Python. 
The choice is due to the fact that this language works great with data processing and allows you to do it flexibly and deftly. 

### Libraries:
* __Tkinter/Customtkinter__ - the main libraries used. With the help of them, an interface for working with data was created. Easy and flexible to use and create the desired functionality, since there was no goal in creating a complex software product with a tricky interface.
* __psycopg2__ - for working with PostgreSQL.
* __pandas__ - for working with data. It was used to process parsed data using dataframes.
* __pandastable__ - for convenient visualization of data from dataframes in the form of tables, similar to Excel.
* __subprocess__, __io__ - for opening PRS-files just from my programm by double-click.
* __datetime__ - for preprocessing datetime.
* __re__ - for working with regular expressions. is used to search for the necessary data in PRS files for parsing.
* __configparser__ - for using config files for connecting to database.
* __math__ - for math processing datas (speed calculation in knots).
* __shapely.geometry__ - to define ICES areas, economic zones and local areas (Barents Sea, Norwegian Sea, Faroe Islands, etc.).
* __docx__ - to export a file with errors, if any occur when checking the correctness of the data.
* __threading__ - to create a progressbar.

 ## Example using
 ### Processing a PRS file for one of the tables (station - it contains meteorological information)
 1. This is the first thing the user sees when the program starts:  
  
![image](https://github.com/Leeralim/Ocean/assets/49206103/0f06ccf1-9d5d-44ee-bd8c-2c582bb0b3cb)  
 
  
2. Select a file using "Файлы" in the menu. Select PRS files:  
  
![image](https://github.com/Leeralim/Ocean/assets/49206103/1f7079bd-4562-4d38-9c0d-3e6854c12a2c)  

  
3. Select all or several necessary files for processing:  
  
![image](https://github.com/Leeralim/Ocean/assets/49206103/4b78b679-9f2a-4a45-bf5e-5ab92b5b6863)  

  
4. We see a list of selected files in the lower window. If you click on them 2 times, the file will open in notepad. To process the file, click on the "Обработать" button:  
  
![image](https://github.com/Leeralim/Ocean/assets/49206103/2a070a59-0f03-4d39-b800-39bb74572bf1)  

  
Processing result:    
  
![image](https://github.com/Leeralim/Ocean/assets/49206103/d1b251b2-7312-40b7-b223-4e2078134196)  
  
![image](https://github.com/Leeralim/Ocean/assets/49206103/51649c06-b433-46fb-80cf-5439e71b156c)  

  
Further, it can be exported to CSV or uploaded to the database.
