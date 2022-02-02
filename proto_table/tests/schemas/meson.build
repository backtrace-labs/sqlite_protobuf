protoc = find_program(join_paths(get_option('prefix'),'bin/protoc-c'), required: true)
protoc_gen = generator(protoc, output: ['@BASENAME@.pb-c.c', '@BASENAME@.pb-c.h'],
                       arguments: ['--proto_path=@CURRENT_SOURCE_DIR@',
                                   '--c_out=@BUILD_DIR@', '@INPUT@'])

protocpp = find_program(join_paths(get_option('prefix'),'bin/protoc'), required: true)
protocpp_gen = generator(protocpp, output: ['@BASENAME@.pb.cc', '@BASENAME@.pb.h'],
                         arguments: ['--proto_path=@CURRENT_SOURCE_DIR@',
                                     '--cpp_out=@BUILD_DIR@', '@INPUT@'])

proto_files = '''
test.proto
'''.split()

proto_table_test_protoc_files = []
proto_table_test_proto_cpp_files = []

foreach proto : proto_files
	proto_table_test_protoc_files += protoc_gen.process(proto)
	proto_table_test_proto_cpp_files += protocpp_gen.process(proto)
endforeach

# We want to build the C++ files separately: we only want to link them
# in for the global message descriptor registry, and they need special
# build flags.
libproto_table_test_proto = static_library('proto_table_test_proto',
	[ proto_table_test_proto_cpp_files ],
	dependencies: [libprotobuf_dep],
	cpp_args: ['-Wno-undef', '-Wno-missing-declarations'])

libproto_table_test_proto_dep = declare_dependency(link_whole: libproto_table_test_proto)

# Build a shared object for testing using the sqlite3 shell.
libproto_table_test_proto_so = shared_library('proto_table_test_proto',
	proto_table_test_proto_cpp_files,
	dependencies: [libprotobuf_dep],
	cpp_args: ['-Wno-undef', '-Wno-missing-declarations'],
	install: false)
