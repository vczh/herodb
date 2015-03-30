mkdir Coverage
cd Coverage
gcov -o ../Obj/ ../../Source/Utility/*.cpp ../*.cpp
lcov --directory ../Obj --capture --output-file lcov.info
genhtml -o HTML lcov.info
echo Generated Files:
ls -la
