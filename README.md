# Please add your team members' names here. 

## Team members' names

1. Student Name: Aman Rajeshkumar Modi

    Student UT EID: arm6433

2. Student Name: Rohit Alamgari

    Student UT EID: rra764

3. Student Name: Davinderpal Toor

    Student UT EID: dst775

 ...

##  Course Name: CS378 - Cloud Computing 

##  Unique Number: 50655

##  Date Created: 9/15/2024


# Add your Project REPORT HERE 


# Project Template

# Running on Laptop     ####

Prerequisite:

- Maven 3

- JDK 1.6 or higher

- (If working with eclipse) Eclipse with m2eclipse plugin installed


The java main class is:

edu.cs.utexas.HadoopEx.WordCount 

Input file:  Book-Tiny.txt  

Specify your own Output directory like 

# Running:




## Create a JAR Using Maven 

To compile the project and create a single jar file with all dependencies: 
	
```	mvn clean package ```


## Run your application
Inside your shell with Hadoop

Running as Java Application (for Task One on small):

```java -jar target/MapReduce-WordCount-example-0.1-SNAPSHOT-jar-with-dependencies.jar taxi-data-sorted-small.csv  output``` 

Running as Java Application (for Task Two or Three on small):

```java -jar target/MapReduce-WordCount-example-0.1-SNAPSHOT-jar-with-dependencies.jar taxi-data-sorted-small.csv intermediateFolder output``` 

Or has hadoop application

```hadoop jar your-hadoop-application.jar edu.cs.utexas.HadoopEx.WordCount arg0 arg1 ... ```



## Create a single JAR File from eclipse



Create a single gar file with eclipse 

*  File export -> export  -> export as binary ->  "Extract generated libraries into generated JAR"
