# BeerReduce

The goal of this project create an hadoop application on this dataset: https://www.kaggle.com/jtrofe/beer-recipes#recipeData.csv.
We try to answer the following questions:
- For each beer family (API, ALE), with which brewing methods do we get the sweetest beer?
- For each beers's color, which one consumed the least amount of water per pound of grain?

## Travis

[![Build Status](https://travis-ci.com/PamplemousseMR/BeerReduce.svg?branch=master)](https://travis-ci.com/PamplemousseMR/BeerReduce)

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

- [Hadoop](https://hadoop.apache.org/) : The Apache Hadoop software library is a framework that allows for the distributed processing of large data sets across clusters of computers using simple programming models.

### Compile

Compile using : `compile.bat JAVA_HOME`, `JAVA_HOME` is a parameter indicating the path of Java. We need this because Hadoop work only if Java is located to a directory without space or special character.

Launch the program by using : `launch.bat JAVA_HOME`.

## Authors

* **MANCIAUX Romain** - *Initial work* - [PamplemousseMR](https://github.com/PamplemousseMR).
* **HANSER Florian** - *Initial work* - [ResnaHF](https://github.com/ResnaHF).

## License

This project is licensed under the GNU Lesser General Public License v3.0 - see the [LICENSE.md](LICENSE.md) file for details.