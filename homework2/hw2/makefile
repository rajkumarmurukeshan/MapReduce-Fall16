default: 
	cd `pwd`/noCombiner/;mvn install
	cd `pwd`/Combiner/;mvn install
	cd `pwd`/inMapCombiner/;mvn install
	cd `pwd`/task2/;mvn install

local:
	cd `pwd`/noCombiner/;java -jar ./target/noCombiner-0.0.1-SNAPSHOT-jar-with-dependencies.jar ./*.csv ./output
	cd `pwd`/Combiner/;java -jar ./target/combiner-0.0.1-SNAPSHOT-jar-with-dependencies.jar ./*.csv ./output
	cd `pwd`/inMapCombiner/;java -jar ./target/inMapCombiner-0.0.1-SNAPSHOT-jar-with-dependencies.jar ./*.csv ./output
	cd `pwd`/task2/;java -jar ./target/task2-0.0.1-SNAPSHOT-jar-with-dependencies.jar ./HW2InputFiles ./output
