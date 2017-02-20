all: mem2fd-perf-test

mem2fd-perf-test: mem2fd-perf-test.cpp
	g++ -std=c++11 $< -o $@
