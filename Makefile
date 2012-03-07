berthad-vfs: berthad-vfs.c
	${CC} -std=c99 -pedantic -Wall $< -o $@\
		$(shell pkg-config --cflags --libs glib-2.0 gthread-2.0)
