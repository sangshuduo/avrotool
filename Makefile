.PHONY: test clean

version = 0.0.1

avro-c: avro-c.o
	gcc -o avro-c avro-c.o -lavro

avro-c.o: avro-c.c
	gcc -c avro-c.c

clean:
	@if [ -f avro-c.o ]; then rm avro-c.o; fi
	@if [ -f avro-c ]; then rm avro-c; fi
