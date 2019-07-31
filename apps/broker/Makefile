ERLANG_PATH = $(shell erl -eval 'io:format("~s", [lists:concat([code:root_dir(), "/erts-", erlang:system_info(version), "/include"])])' -s init stop -noshell)

all:
	# gcc -fpic -shared -o priv/entangled.so c_src/entangled_nif.c c_src/utils/memset_safe.c c_src/common/model/transaction.c c_src/common/trinary/*.c c_src/common/crypto/curl-p/digest.c c_src/common/crypto/curl-p/const.c c_src/common/crypto/curl-p/curl_p.c -I "$(ERLANG_PATH)"
	gcc -fpic -shared -o priv/nifs.so c_src/common/trinary/*.c c_src/common/crypto/curl-p/const.c c_src/utils/memset_safe.c c_src/common/crypto/curl-p/curl_p.c c_src/nifs.c c_src/common/crypto/curl-p/ptrit.c -I "$(ERLANG_PATH)"

clean:
	rm -r "priv/nifs.so"
