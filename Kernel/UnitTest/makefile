# This file is generated from "./makefile.debug-clang.makegen" by Vczh Makefile Generator

# Config
.PHONY : all clean

# Targets
bin_TARGET = ././Bin/
obj_TARGET = ././Obj/

# Folders
DATABASE_BASIC_DIR = ./../Source/./
DATABASE_BASIC_h = $(DATABASE_BASIC_DIR)DatabaseVlppReferences.h
DATABASE_UTILITY_DIR = ./../Source/./Utility/
DATABASE_UTILITY_cpp = $(wildcard $(DATABASE_UTILITY_DIR)*.cpp)
DATABASE_UTILITY_h = $(wildcard $(DATABASE_UTILITY_DIR)*.h)
UNITTEST_DIR = ././
UNITTEST_cpp = $(wildcard $(UNITTEST_DIR)*.cpp)
UNITTEST_h = $(wildcard $(UNITTEST_DIR)*.h)
VLPP_DIR = ./../Source/./
VLPP_cpp = $(VLPP_DIR)Vlpp.cpp
VLPP_h = $(VLPP_DIR)Vlpp.h

# Output Categories
DATABASE_UTILITY_o = $(patsubst $(DATABASE_UTILITY_DIR)%.cpp, $(obj_TARGET)%.o, $(DATABASE_UTILITY_cpp))
UNITTEST_o = $(patsubst $(UNITTEST_DIR)%.cpp, $(obj_TARGET)%.o, $(UNITTEST_cpp))
VLPP_o = $(patsubst $(VLPP_DIR)%.cpp, $(obj_TARGET)%.o, $(VLPP_cpp))

# All
ALL_o = $(DATABASE_UTILITY_o) $(VLPP_o) $(UNITTEST_o)
all : $(ALL_o)
	clang++ -std=c++11 -pthread -o $(bin_TARGET)UnitTest  $(ALL_o)

# Dependencies
# $(DATABASE_BASIC_h) : $(VLPP_h)
# $(DATABASE_UTILITY_h) : $(DATABASE_BASIC_h)
# $(DATABASE_UTILITY_cpp) : $(DATABASE_UTILITY_h)
# $(VLPP_cpp) : $(VLPP_h)
# $(UNITTEST_h) : $(DATABASE_BASIC_h) $(DATABASE_UTILITY_h)
# $(UNITTEST_cpp) : $(UNITTEST_h)

# Rules
$(DATABASE_UTILITY_o) : $(obj_TARGET)%.o : $(DATABASE_UTILITY_DIR)%.cpp $(DATABASE_BASIC_h) $(DATABASE_UTILITY_h) $(VLPP_h)
	clang++ -std=c++11 -g -o $@ -c $<
$(VLPP_o) : $(obj_TARGET)%.o : $(VLPP_DIR)%.cpp $(VLPP_h)
	clang++ -std=c++11 -g -o $@ -c $<
$(UNITTEST_o) : $(obj_TARGET)%.o : $(UNITTEST_DIR)%.cpp $(DATABASE_BASIC_h) $(DATABASE_UTILITY_h) $(UNITTEST_h) $(VLPP_h)
	clang++ -std=c++11 -g -o $@ -c $<

# Clean
clean:
	rm $(bin_TARGET)* -rf
	rm $(obj_TARGET)* -rf
