berthad-vfs: berthad-vfs.c
	${CC} -std=c99 -pedantic -Wall -O2 $< -o $@ `pkg-config --cflags --libs glib-2.0 gthread-2.0`
