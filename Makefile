all:
	gcc pgsql-dbus.c -ggdb -o pgsql-dbus `pkg-config --cflags --libs libsystemd` -I/home/ioguix/usr/local/pgsql/REL_13_STABLE/include -lpq
