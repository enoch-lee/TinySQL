# Summary
Tiny SQL is a simple SQL interpreter. The interpreter includes parser, logical query plan generator and physical query plan generator. The StorageManager library is provided to support physical execution of the  Tiny-SQL interpreter.

# How to run the project
1.  It is suggested to use IDE such as [IntelliJ](https://www.jetbrains.com/idea/) to open the project.
2.  Use Test as main Class and run the program.
3.  Input output file name.
4.  Choose command line or file mode.
	* Input Tiny SQL statement at a line.
    * Input a file name which contains many Tiny-SQL statements (e.g test.txt).
5.  The console will print every successful query and results of SELECT query.

> **Note**:
> - test.txt contains all test cases.
> - output.txt saved output of test.txt

**Contribuor**: [Zishuo](https://github.com/enoch-lee) and [Jicheng](https://github.com/TonyGongjc)

**Acknowledgements**: We get some ideas of implementation from [Yongle](https://github.com/lyltj2010) and [Lihao](https://github.com/leoloe326)