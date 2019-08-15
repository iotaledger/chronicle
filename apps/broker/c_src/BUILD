package(default_visibility = ["//visibility:public"])
cc_binary(
    name = "nifs.so",
    srcs = ["nifs.c"],
    linkshared = 1,
    deps = [
        "@keccak",
	"//common/crypto/curl-p:ptrit",
        "//common/trinary:trit_ptrit",
	"//common/trinary:trit_tryte",
        "//common/model:bundle",
	":erl_nif",
    ],
)

cc_library(
    name = "erl_nif",
    hdrs = [
        "erl_nif.h", "erl_drv_nif.h", "erl_int_sizes_config.h","erl_nif_api_funcs.h"
    ],
)
