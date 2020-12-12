#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <stddef.h>
#include <stdbool.h>
#include <arpa/inet.h>
#include <systemd/sd-bus.h>

#include "libpq-fe.h"
#include "server/catalog/pg_type_d.h"
//#include "fe_utils/print.h"

/*
 * Quick definition and boiler plate:
 *   http://0pointer.net/blog/the-new-sd-bus-api-of-systemd.html
 *
 * API manpages:
 *   https://www.freedesktop.org/software/systemd/man/index.html
 *
 * DBus type specs:
 *   https://dbus.freedesktop.org/doc/dbus-specification.html#type-system
 *   https://github.com/systemd/systemd/blob/master/src/systemd/sd-bus-protocol.h
 *
 * exec example:
 *   busctl --user call org.postgresql.instance /org/postgresql/instance org.postgresql.instance Query s "SELECT 10::int2 as blah"
 */

typedef struct context {
	char * host;
	uint16_t port;
} context;

static const char oid_to_signature(Oid oid)
{
	switch (oid)
	{
		case BOOLOID:	return 'b';
		case INT2OID:	return 'n';
		case OIDOID:
		case INT4OID:	return 'i';
		case INT8OID:	return 'x';
		case FLOAT4OID:
		case FLOAT8OID:	return 'd';
		case XMLOID:
		case JSONOID:
		case VARCHAROID:
		case BPCHAROID:
		case NAMEOID:
		case TEXTOID:	return 's';
		default:		return 's';
	}

	/* never reached */
	return 's';
}

static int sd_bus_message_append_pgsql_value(sd_bus_message *m, char sig, Oid oid, char *value)
{
	int rc;

	switch (oid)
	{
		case BOOLOID:	{
			int b;
			b = (*value == 't')? 1:0;
			return sd_bus_message_append_basic(m, sig, &b);
		}
		case INT2OID: {
			int16_t s;
			s = (int16_t) strtol(value, NULL, 10);
			return sd_bus_message_append_basic(m, sig, &s);
		}
		case OIDOID:
		case INT4OID: {
			int32_t i;
			i = (int32_t) strtol(value, NULL, 10);
			return sd_bus_message_append_basic(m, sig, &i);
		}
		case INT8OID: {
			int64_t q;
			q = (int64_t) strtoll(value, NULL, 10);
			return sd_bus_message_append_basic(m, sig, &q);
		}
		case FLOAT4OID:
		case FLOAT8OID:	{
			double d;
			d = strtod(value, NULL);
			return sd_bus_message_append_basic(m, sig, &d);
		}
		case XMLOID:
		case JSONOID:
		case VARCHAROID:
		case BPCHAROID:
		case NAMEOID:
		case TEXTOID: {
			return sd_bus_message_append_basic(m, sig, value);
		}
		default:		return -1;
	}

	/* never reached */
	return -1;
}

static int method_ping(sd_bus_message *m, void *userdata, sd_bus_error *ret_error) {
	context *c = userdata;
	const char *keys[3] = { "host", "port", NULL };
	const char *vals[3];
	char port[6];
	int rc;

	sprintf(port, "%d", c->port);

	vals[0] = c->host;
	vals[1] = port;
	vals[2] = NULL;
	// fprintf(stderr, "%s: %s, %s: %s\n", keys[0], vals[0], keys[1], vals[1] );

	rc = PQpingParams(keys, vals, 0);
	switch (rc)
	{
		case PQPING_OK:
			fprintf(stderr, "accepting connections\n");
			break;
		case PQPING_REJECT:
			fprintf(stderr, "rejecting connections\n");
			break;
		case PQPING_NO_RESPONSE:
			fprintf(stderr, "no response\n");
			break;
		case PQPING_NO_ATTEMPT:
			fprintf(stderr, "no attempt\n");
			break;
		default:
			fprintf(stderr, "unknown\n");
	}

	return sd_bus_reply_method_return(m, "q", rc);
}


static int method_query(sd_bus_message *m, void *userdata, sd_bus_error *ret_error)
{
	context		   *c = userdata;
	sd_bus_message *reply = NULL;
	sd_bus		   *bus = NULL;
	const char	   *keys[3] = { "host", "port", NULL };
	const char	   *vals[3];
	char			port[6];
	PGconn		   *conn;
	PGresult	   *res;
	char		   *query;
	int				rc, nFields, i, j;

	rc = sd_bus_message_read(m, "s", &query);
	if (rc < 0)
		return sd_bus_reply_method_errorf(m, SD_BUS_ERROR_INVALID_ARGS, "Expected interface parameter");

	bus = sd_bus_message_get_bus(m);

	sprintf(port, "%d", c->port);

	vals[0] = c->host;
	vals[1] = port;
	vals[2] = NULL;
	fprintf(stderr, "connecting to %s: %s, %s: %s\n", keys[0], vals[0], keys[1], vals[1] );

	conn = PQconnectdbParams(keys, vals, 0);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		PQfinish(conn);
		goto end;
	}

	res = PQexec(conn, query);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "query failed: %s", PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		goto end;
	}

	fprintf(stderr, "query succeed!\n");

	nFields = PQnfields(res);

	rc = sd_bus_message_new_method_return(m, &reply);
	if (rc < 0) return rc;

	/* open array of dictionary string->variant */
	rc = sd_bus_message_open_container(reply, 'a', "{sv}");
	if (rc < 0) return rc;

	/* append one row */
	for (i = 0; i < nFields; i++)
	{
		Oid oid = PQftype(res, i);
		char sig[] = { oid_to_signature(oid), '\0' };

		// fprintf(stderr, "(%c)%-15s: %-15s\n", sig[0], PQfname(res, i), PQgetvalue(res, 0, i));

		/* append one field*/
		/* open dict entry og string->variant */
		rc = sd_bus_message_open_container(reply, 'e', "sv");
		if (rc < 0) return rc;

		/* name of the field as dict key */
		rc = sd_bus_message_append(reply, "s", PQfname(res, i));
		if (rc < 0) return rc;

		/* open variant with given type for field */
		rc = sd_bus_message_open_container(reply, 'v', sig);
		if (rc < 0) return rc;

		/* append the value to the variant */
		rc = sd_bus_message_append_pgsql_value(reply, sig[0], oid, PQgetvalue(res, 0, i));
		if (rc < 0) return rc;

		/* close the variant for field type+value */
		rc = sd_bus_message_close_container(reply);
		if (rc < 0) return rc;

		/* close dict entry for field */
		rc = sd_bus_message_close_container(reply);
		if (rc < 0) return rc;
	}

	/* close one row */
	rc = sd_bus_message_close_container(reply);
	if (rc < 0) return rc;

	PQclear(res);
	PQfinish(conn);

	rc = sd_bus_send(bus, reply, NULL);
	return rc;

end:
	return sd_bus_reply_method_return(m, NULL);
}

/* implements the org.postgresql.instance interface */
static const sd_bus_vtable instance_vtable[] = {
	SD_BUS_VTABLE_START(0),
	SD_BUS_METHOD(
		"Ping",
		NULL,
		"q",		// uint16
		method_ping,
		SD_BUS_VTABLE_UNPRIVILEGED),
	SD_BUS_METHOD(
		"Query",
		"s",
		"a{sv}",
		method_query,
		SD_BUS_VTABLE_UNPRIVILEGED),
	SD_BUS_WRITABLE_PROPERTY(
		"Port",
		"q",		// uint16: max 65536 ports
		NULL, NULL,	// get & set function useless for basic types
		offsetof(context, port),
		SD_BUS_VTABLE_PROPERTY_EMITS_CHANGE),
	SD_BUS_WRITABLE_PROPERTY(
		"Host",
		"s",
		NULL, NULL,	// FIXME: implement them to avoid overflow
		offsetof(context, host),
		SD_BUS_VTABLE_PROPERTY_EMITS_CHANGE),

	SD_BUS_VTABLE_END
};

int main(int argc, char *argv[]) {
	sd_bus_slot *slot = NULL;
	sd_bus *bus = NULL;
	context c;
	int rc;

	c.port = 15433;
	c.host = strdup("/tmp");

	/* Connect to the user bus this time */
	rc = sd_bus_open_user(&bus);
	if (rc < 0) {
		fprintf(stderr, "Failed to connect to system bus: %s\n", strerror(-rc));
		goto finish;
	}

	/* Install the object */
	rc = sd_bus_add_object_vtable(bus,
				&slot,
				"/org/postgresql/instance",  /* object path */
				"org.postgresql.instance",   /* interface name */
				instance_vtable,
				&c);
	if (rc < 0) {
		fprintf(stderr, "Failed to issue method call: %s\n", strerror(-rc));
		goto finish;
	}

	/* Take a well-known service name so that clients can find us */
	rc = sd_bus_request_name(bus, "org.postgresql.instance", 0);
	if (rc < 0) {
		fprintf(stderr, "Failed to acquire service name: %s\n", strerror(-rc));
		goto finish;
	}

	for (;;) {
		/* Process requests */
		rc = sd_bus_process(bus, NULL);
		if (rc < 0) {
			fprintf(stderr, "Failed to process bus: %s\n", strerror(-rc));
			goto finish;
		}
		if (rc > 0) /* we processed a request, try to process another one, right-away */
			continue;

		/* Wait for the next request to process */
		rc = sd_bus_wait(bus, (uint64_t) -1);
		if (rc < 0) {
			fprintf(stderr, "Failed to wait on bus: %s\n", strerror(-rc));
			goto finish;
		}
	}

finish:
	sd_bus_slot_unref(slot);
	sd_bus_unref(bus);

	return rc < 0 ? EXIT_FAILURE : EXIT_SUCCESS;
}
