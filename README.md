hadoop-louvain-community
========================

Map Reduce Implementation of a community detection algorithm extending Louvain method for community detection.

Compiling
----------
This is a maven project. 
<code>$mvn clean install</code>
 will complie and create louvain-mr-0.1.jar in target directory. 

Data Conversion
---------------
This algorithm expects data in metis unweighted graph format. See [here](http://people.sc.fsu.edu/~jburkardt/data/metis_graph/metis_graph.html) for more information.

convert the data by using following command

<code>$java -cp "path to louvain-mr-0.1.jar" edu.usc.pgroup.louvain.hadoop.tools.DataConverter "path to metis graph file" "path to metis partition file" number_of_partitions</code>

Example:
 
<code>$java -cp louvain-mr-0.1.jar edu.usc.pgroup.louvain.hadoop.tools.DataConverter /home/charith/data/4elt.graph /home/charith/data/4elt.graph.part.5 5</code>

This will create set of partitioned files in /home/charith/data/ directory. 

Running Hadoop Job
------------------

Use Following commeand to run the Hadoop Job

<code>$hadoop jar "path to louvain-mr-0.1.jar" edu.usc.pgroup.louvain.hadoop.LouvainMR "input directory path where partition files are located"  "output path" -1 "true to enable loggin false to disable logging"</code>

Example command: 

<code>$./bin/hadoop jar louvain-mr-0.1.jar edu.usc.pgroup.louvain.hadoop.LouvainMR /home/charith/software/hadoop-2.2.0/input /home/charith/software/hadoop-2.2.0/output -1 true</code>

Community mappings will be written to output path once the job is complete. 




 
 
