sqlite_protobuf_src_dir = 'src'
sqlite_protobuf_include_dir = include_directories('include')

sqlite_protobuf_src_files = '''
	extension_main.cpp
	protobuf_enum.cpp
	protobuf_extract.cpp
	protobuf_load.cpp
	protobuf_json.cpp
	protobuf_text.cpp
	utilities.cpp
'''.split()

_sqlite_protobuf_src_files = []
foreach s : sqlite_protobuf_src_files
	_sqlite_protobuf_src_files += join_paths(sqlite_protobuf_src_dir, s)
endforeach

sqlite_protobuf_lib = static_library('sqlite_protobuf',
	_sqlite_protobuf_src_files,
	include_directories: sqlite_protobuf_include_dir,
	dependencies: [libprotobuf_dep, libdl_dep],
	cpp_args: ['-Wno-undef'])

libsqlite_protobuf_dep = declare_dependency(link_whole: sqlite_protobuf_lib,
	include_directories: sqlite_protobuf_include_dir)

libsqlite_protobuf_header_dep = declare_dependency(
	include_directories: sqlite_protobuf_include_dir)

sqlite_protobuf_so = shared_library('sqlite_protobuf',
	_sqlite_protobuf_src_files,
	include_directories: sqlite_protobuf_include_dir,
	dependencies: [libprotobuf_dep, libdl_dep],
	cpp_args: ['-Wno-undef'],
	install: true)
