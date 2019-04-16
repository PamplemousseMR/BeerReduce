# BeerReduce

The goal of this project is to create an hadoop application on this dataset: https://www.kaggle.com/jtrofe/beer-recipes#recipeData.csv.
We try to answer the following questions:
- For each beer family (API, ALE), with which brewing methods do we get the most sweetests beers?
- For each beer family, how many beer consumed the least amount of water per pound of grain grouped by quarter of beers's color (rounded up) ?

## Travis

[![Build Status](https://travis-ci.com/PamplemousseMR/BeerReduce.svg?branch=master)](https://travis-ci.com/PamplemousseMR/BeerReduce)

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

- [Hadoop](https://hadoop.apache.org/) : The Apache Hadoop software library is a framework that allows for the distributed processing of large data sets across clusters of computers using simple programming models.
- For Linux users : set these environement variables : JAVA_HOME, HADOOP_CLASSPATH and HADOOP_HOME

### Compilation

Windows:

	Compile using : `compile.bat JAVA_HOME`, `JAVA_HOME` is a parameter indicating the path of Java. We need this because Hadoop work only if Java is located to a directory without space or special character.

Linux:

	Compile using : `Makefile`. It will compile each java source file in src folder and automatically generate launchers.

### Use

Windows:

	Launch the program by using : `launchBestIPA.bat JAVA_HOME`. This launcher allow to answer the first question.
	Launch the program by using : `launchDarkestBeer.bat JAVA_HOME`. This launcher allow to answer the second question.

Linux:

	Launch the program by using : `launchBestIPA.sh`. This launcher allow to answer the first question.
	Launch the program by using : `launchDarkestBeer.sh`. This launcher allow to answer the second question.

Each launcher creates a new directory (respectively `bestIPAResult` and `darkestBeerResult`) where you can find a file named `part-r-00000` that contains the answer to the corresponding question.

## Authors

* **MANCIAUX Romain** - *Initial work* - [PamplemousseMR](https://github.com/PamplemousseMR).
* **HANSER Florian** - *Initial work* - [ResnaHF](https://github.com/ResnaHF).

## License

This project is licensed under the GNU Lesser General Public License v3.0 - see the [LICENSE.md](LICENSE.md) file for details.