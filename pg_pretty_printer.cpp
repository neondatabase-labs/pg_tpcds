#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"

template <typename T>
T my_add(T a, T b)
{
	return a * b;
}

extern "C"
{
	PG_MODULE_MAGIC;

	PG_FUNCTION_INFO_V1(pg_pretty_printer);

#define PP_FUNC_HEAD(func) PG_PP_##func

	PGDLLEXPORT char *PP_FUNC_HEAD(Node)(Node *node)
	{
		return "Hello, pg_pretty_printer!";
	}

#undef PP_FUNC_HEAD

	/*
	 * show current supported printers
	 */
	Datum pg_pretty_printer(PG_FUNCTION_ARGS)
	{
		int v = 0;
		int32 a = PG_GETARG_INT32(0);
		int32 b = PG_GETARG_INT32(1);

		v = my_add(a, b);

		PG_RETURN_INT32(v);
	}
}