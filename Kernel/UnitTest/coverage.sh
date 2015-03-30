mkdir Bin
mkdir Obj
cp makefile makefile~
MakeGen debug makefile
make clean
make
cp makefile~ makefile
rm makefile~
Bin/UnitTest
mkdir Coverage
cd Coverage
gcov -o ../Obj/ ../../Source/Utility/*.cpp ../*.cpp
lcov --directory ../Obj/ --capture --output-file lcov.info
genhtml -o HTML lcov.info
echo Generated Files:
ls -la
