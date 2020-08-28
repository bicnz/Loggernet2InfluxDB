# Loggernet2InfluxDB
Python script to parse Campbell Scientific Loggernet .dat files and publish to InfluxDB for visualisation in Grafana.

***
### Usage

./Loggernet_to_InfluxDB.py `<ID>` `<FilePath>` `<NumberOfRows>`

If number of rows argument isn't provided, the whole file will be processed.

**Example:** ./Loggernet_to_InfluxDB.py Telemetry_Site1 /mnt/telemetry/site1.dat 50

***
