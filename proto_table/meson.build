proto_table_src_dir = 'src'
proto_table_include_dir = include_directories('include')

proto_table_src_files = '''
	proto_table.c
'''.split()

_proto_table_src_files = []
foreach s : proto_table_src_files
	_proto_table_src_files += join_paths(proto_table_src_dir, s)
endforeach

proto_table_c_args = []

proto_table_lib_deps = [
	libprotobuf_c_dep,
	libumash_dep,
	libbackend_rust_header_dep # for sqlite3.h
]

proto_table_lib = static_library('proto_table', _proto_table_src_files,
	include_directories: proto_table_include_dir,
	dependencies: proto_table_lib_deps,
	c_args: proto_table_c_args)

libproto_table_dep = declare_dependency(link_with: proto_table_lib,
	include_directories: proto_table_include_dir)

libproto_table_header_dep = declare_dependency(
	include_directories: proto_table_include_dir)
