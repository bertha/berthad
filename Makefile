CFLAGS=-Wall -c -pedantic \
		`pkg-config --cflags glib-2.0` \
		`pkg-config --cflags gthread-2.0`
LDFLAGS=-pedantic \
		`pkg-config --libs glib-2.0` \
		`pkg-config --libs gthread-2.0`

berthad-vfs: berthad-vfs.o
	${CC} ${LDFLAGS} $< -o $@

%.o: %.c
	${CC} ${CFLAGS} $< -o $@
