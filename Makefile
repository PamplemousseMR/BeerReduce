SRC=src
BIN=beerReduceBin
OBJ:=$(shell ls $(SRC) | grep .java | sed -e 's/\([^ ]*\)\.java/$(BIN)\/\1.jar/g')
HADOPT=com.sun.tools.javac.Main

all : $(OBJ)

$(BIN)/%.jar : $(SRC)/%.java
	mkdir -p $(BIN)
	hadoop $(HADOPT) -d $(BIN) $<
	jar -cvf $@ -C $(BIN)/ .
	echo 'make requestClass=$* launch \n' > launch$*.sh

clean :
	rm -Rf beerReduceBin launch*.sh
	
launch :
	rm -Rf $(requestClass)Result 2> /dev/null
	hadoop jar $(BIN)/$(requestClass).jar $(requestClass) data/recipeData.csv $(requestClass)Result
