#include "erl_nif.h"

#include "common/crypto/curl-p/ptrit.h" // ptrit_curl impl
#include "common/trinary/trit_ptrit.h" // trits <-> ptrits conversion
#include "common/trinary/trit_tryte.h" // trits <-> trytes conversion

#define DEBUG 3

#if defined(DEBUG) && DEBUG > 0
 #define DEBUG_PRINT(fmt, args...) fprintf(stderr, "DEBUG: %s:%d:%s(): " fmt, \
    __FILE__, __LINE__, __func__, ##args)
#else
 #define DEBUG_PRINT(fmt, args...) /* Don't do anything in release builds */
#endif

typedef struct PECurl_s {
  PCurl curl; // curl state
  ptrit_t acc[243]; // accumulator for the current tx chunks
} PECurl;

ErlNifResourceType* RES_TYPE;
ERL_NIF_TERM atom_ok;
ERL_NIF_TERM atom_true;
ERL_NIF_TERM atom_false;

void
free_res(ErlNifEnv* env, void* res)
{
  // free curl_p resource
  ptrit_curl_reset(res); //?
}

static int
open_resource(ErlNifEnv* env)
{
    const char* mod = "Elixir.Nifs";
    const char* name = "PECurl";
    int flags = ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER;

    RES_TYPE = enif_open_resource_type(env, mod, name, free_res, flags, NULL);
    if(RES_TYPE == NULL) return -1;
    return 0;
}
// erlang nif related functions
static int
load(ErlNifEnv* env, void** priv, ERL_NIF_TERM load_info)
{
    if(open_resource(env) == -1) return -1;

    atom_ok = enif_make_atom(env, "ok");
    atom_true = enif_make_atom(env, "true");
    atom_false = enif_make_atom(env, "false");
    return 0;
}

// Erlang requires that we re-open resources on re-initialisation.
static int
reload(ErlNifEnv* env, void** priv, ERL_NIF_TERM load_info)
{
    if(open_resource(env) == -1) return -1;
    return 0;
}

static int
upgrade(ErlNifEnv* env, void** priv, void** old_priv, ERL_NIF_TERM load_info)
{
    if(open_resource(env) == -1) return -1;
    return 0;
}

// curl_p_81_init_nif function which alloc resource and return pointer to
static ERL_NIF_TERM
curl_p_81_init_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    PECurl* pecurl;
    ERL_NIF_TERM ret;

    if(argc != 0)
    {
        return enif_make_badarg(env);
    }

    // alloc memory resource with sizeof(PECurl) typestruct
    pecurl = enif_alloc_resource(RES_TYPE, sizeof(PECurl));
    // if pecurl == NULL return bad argument.
    if(pecurl == NULL) return enif_make_badarg(env);
    ret = enif_make_resource(env, pecurl);
    enif_release_resource(pecurl);
    // init curl_p
    pecurl->curl.type = CURL_P_81;
    ptrit_curl_init(&pecurl->curl, CURL_P_81);
    // return {:ok, curl_p resource ref}
    return enif_make_tuple2(env, atom_ok, ret);
}

// args: PECurl *pecurl
static ERL_NIF_TERM
absorb(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    PECurl *pecurl;
    if(argc != 1)
    {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], RES_TYPE, (void**) &pecurl))
    {
	return enif_make_badarg(env);
    }
    // absorb into ptrit_curl
    ptrit_curl_absorb(&pecurl->curl, pecurl->acc, 243);

    return atom_ok;
}

// args: PECurl *pecurl
static ERL_NIF_TERM
squeeze(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    PECurl *pecurl;
    if(argc != 1)
    {
      return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], RES_TYPE, (void**) &pecurl))
    {
    	return enif_make_badarg(env);
    }
    // squeeze from ptrit_curl
    // the same `acc` buffer is reused!
    ptrit_curl_squeeze(&pecurl->curl, pecurl->acc, 243);
    return atom_ok;
}

// args: PECurl *pecurl, int tx_count (0 <= tx_count < 64), tryte_t const tx_chunk_trytes[81 * tx_count]
static ERL_NIF_TERM
add_trytes(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    PECurl *pecurl;
    int tx_count; // number of chunks
    ErlNifBinary in; // in.data(trytes) of the chunk
    if(argc != 3)
    {
        return enif_make_badarg(env);
    }
    // get res pointer to get the resource
    if(!enif_get_resource(env, argv[0], RES_TYPE, (void**) &pecurl))
    {
	     return enif_make_badarg(env);
    }
    // get tx_count
    if(!enif_get_int(env, argv[1], &tx_count))
    {
       return enif_make_badarg(env);
    }
    // get trytes as binary
    if(!enif_inspect_binary(env, argv[2], &in))
    {
      return enif_make_badarg(env);
    }
    // add trytes
    tryte_t const *tx_chunk = (tryte_t const *)in.data;
    for(int tx_index = 0; tx_index < tx_count; ++tx_index, tx_chunk += 81)
    {
      trit_t trits[243];
      trytes_to_trits(tx_chunk, trits, 81); // `length` argument is the length of trytes
      trits_to_ptrits(trits, pecurl->acc, tx_index, 243);
    }

    return atom_ok;
}

// args: PECurl *pecurl, int tx_count (0 <= tx_count < 64), tryte_t tx_hash_trytes[81 * tx_count]
static ERL_NIF_TERM
get_trytes(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    PECurl *pecurl;
    int tx_count; // tx_count of the chunk
    ErlNifBinary out; // out.data(trytes) of the chunk
    if(argc != 3)
    {
        return enif_make_badarg(env);
    }
    // get res pointer to get the resource
    if(!enif_get_resource(env, argv[0], RES_TYPE, (void**) &pecurl))
    {
	     return enif_make_badarg(env);
    }
    // get tx_count of the chunk
    if(!enif_get_int(env, argv[1], &tx_count))
    {
       return enif_make_badarg(env);
    }
    enif_alloc_binary(81, &out);
    //strncpy((tryte_t*)out.data, TRYTES, NUM_TRYTES_SERIALIZED_TRANSACTION);
    // get trytes
    tryte_t *tx_hash = (tryte_t *)out.data;
    ERL_NIF_TERM result [tx_count];
  //  DEBUG_PRINT("Debug level: %d", tx_count);
    for(int tx_index = 0; tx_index < tx_count; ++tx_index, tx_hash += 81)
    {
      trit_t trits[243];
      ptrits_to_trits(pecurl->acc, trits, tx_index, 243);
      trits_to_trytes(trits ,tx_hash, 243); // `length` argument is the length of the trits, not trytes
      result[tx_index] = atom_true;
    }
    return enif_make_list_from_array(env, result,  tx_count);
}


static ErlNifFunc nif_funcs[] = {
    {"curl_p_init", 0, curl_p_81_init_nif},
    {"absorb", 1, absorb},
    {"squeeze", 1, squeeze},
    {"add_trytes", 3, add_trytes},
    {"get_trytes", 3, get_trytes}
};

ERL_NIF_INIT(Elixir.Nifs, nif_funcs, &load, &reload, &upgrade, NULL);
