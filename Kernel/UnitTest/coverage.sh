mkdir Coverage
gcov -o ./Obj/ ../Source/Utility/*.cpp *.cpp
mv *.gcov ./Coverage/
