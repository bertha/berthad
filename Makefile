CFLAGS=-Wall -c `pkg-config --cflags glib-2.0` -pedantic
LDFLAGS=`pkg-config --libs glib-2.0` -pedantic

berthad-vfs: berthad-vfs.o
	${CC} ${LDFLAGS} $< -o $@

%.o: %.c
	${CC} ${CFLAGS} $< -o $@
