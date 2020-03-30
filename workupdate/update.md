# Work Update

## Docker Setup
- [x] Installation of Docker
- [x] Setup of Flexter on dockers
- [x] Getting used to running docker commands

## Flexter - Basic Understanding
- [x] Concepts of Terminology
	1.	Source Data, Source Schema, Source, Data Point, Statistics, Incremental Statistics, Target Schema, Optimisation ( Reuse and Elevate ), Target Format, Target data, Metadata DB
	2.	Data Flow, Conversion, Source and Target Connection
- [x] Understanding Flexter Conversion Process
	1.	Creation of Data Flow
		1.	Source Schema Available
		2.	No Source Schema Available
		3.	Source Schema and Source Data Available
	2.	Conversion Task
	
- [x] Flexter Modules
	- [x] Create data flow with source schema only. XSD2ER ( Obtain logical ID )
	- [x] Create data flow with source Data XML2ER 
		- [x] Obtain Origin ID - Statistics
		- [x] Obtain Logical ID -  Data Flow value required in further steps
		- [x] Incremental Statistics by using x and e switches. Usage of g switch at the end to create data flow (lofical ID obtained)
	- [x] Creating data flow with source data and source schema
		- [x] Creating statistics with source data as discussed above ( Can be incremental also ) to obtain Origin ID
		- [x] Using k switch and above origin ID with XSD2ER and g switch to obtain Data Flow(Logical ID)
	- [ ] Data Conversion
		- [ ] Batch Mode
			- [x] Generic batch mode runs
			- [ ] Testing all output formats
			- [ ] Testing all compression modes
			- [ ] Memory settings
			- [x] CPU settings. Have worked only on Local vCPUs
		- [x] Streaming Mode
			- [x] Generic streaming mode runs
	- [ ] Source Connection
		- [ ] Batch Mode
			- [x] File:
			- [ ] http/s
			- [ ] ftp
			- [ ] HDFS
			- [ ] JDBC
			- [ ] s3a
		- [ ] Stream Mdoe
			- [x] File:
			- [ ] HDFS
		- [x] XML/JSON in directory
		- [ ] XML/JSON in tabular files
		- [ ] XML/JSON in database table
			- [ ] JDBC
			- [ ] Hive
	- [ ] Target Connection
		- [x] File Based Target Connection
		- [ ] Hive as Target Connection
		- [ ] JDBC as Target Connection
	- [x] Logging
	- [ ] Merging and Converting Files ( MERGE2ER)
	