# USVehicleDataAnalysis
The Repository contain Py spark code to analyze the data of the accidents in United States


The data set consists of 6 files. The metadata of these files is as follows


![image](https://user-images.githubusercontent.com/42976192/216846174-98bfb1f7-1963-4403-ac75-c75f5fd6fd93.png)


![image](https://user-images.githubusercontent.com/42976192/216846201-059b4685-b8c0-4e02-b849-f3c5696c800b.png)


![image](https://user-images.githubusercontent.com/42976192/216846215-33ff1af3-5e22-42b5-8dda-ee4a8dc43927.png)


![image](https://user-images.githubusercontent.com/42976192/216846223-07347aa8-add5-4eb9-9941-eb4a70751809.png)


![image](https://user-images.githubusercontent.com/42976192/216846257-0d097783-6a90-46d4-a95b-ce1b1e3ff10e.png)


![image](https://user-images.githubusercontent.com/42976192/216846281-2ea6b508-1ac5-4c0a-89cd-b6449051c78a.png)


Using these data sets analytics are performed to answer the below question

1.	Analytics 1: Find the number of crashes (accidents) in which number of persons killed are male?
2.	Analysis 2: How many two wheelers are booked for crashes? 
3.	Analysis 3: Which state has highest number of accidents in which females are involved? 
4.	Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
5.	Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
6.	Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
7.	Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
8.	Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)

## Folder Structure
1. code - Contains the actual code main.py
2. data - Conatains the datasets as csv files
3. config - Contains the config file config.yaml 

## Runbook

Clone the repo and follow these steps:


1. Install python and spark on your local machine. Also do install the packages wheel, pyspark and PyYAML.
Post installation of python run the following commands on the terminal
```
pip install wheel
pip install pyspark
pip install PyYAML
```
2. Go to the project directory
```
cd USVehicleDataAnalysis
```
3. Make sure spark is installed in your local machine and environmanetal variable SPARK_HOME is set. On the terminal Run the following code to execute the code

```
spark-submit --master local[*] code/main.py
```
