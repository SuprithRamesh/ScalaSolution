# Splitting Streams

```
This mini-project was undertaken for Sonra   
```

##Preconditions:
1.	Scala Programming
2.	Structured Programming to be implemented
3.	Usage of sbt to package code
4.	Executable to run with spark-submit

##Conditions:
1.	Directory path taken as arguments ($ spark-submit application.jar inputdir outputdir )
2.	Application listens to inputdir for new files
3.	Application creates output files based on certain parameters as given in below image
![alt text](./Filtering.JPG "")
4.	Using structured streams to observe new changes to input directory and run the filtering againq
5.	Should run until user stops the Application

##Post Condition:
1.	No duplicates or missing contents after closing the Application
2.	If Application is run again, the existing output directory is overwritten

##Evaluation Criteria:
1.	programming style
	..*	code structure & style
	..*	handling exceptions (ie. parameter validation, input data validation, runtime errors)
2.	completeness of the solution:
	..*	Spark framework familiarity
	..*	Streaming utilization
	..*	correctness of the output
	..*	(optional/bonus) conversion to other Spark supported format as output (ie. json, parquet etc...)

