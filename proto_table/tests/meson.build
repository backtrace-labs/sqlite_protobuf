subdir('schemas')

proto_table_tests = '''
	result_list
	message
'''.split()

foreach t : proto_table_tests
	e = executable('proto_table_test_' + t,
		[ t + '.c', proto_table_test_protoc_files ],
		dependencies: [
			libproto_table_test_proto_dep,
			libproto_table_dep,

			proto_table_lib_deps,
			libsqlite_protobuf_dep,
			libbackend_rust_dep, # sqlite3
		],
		link_language: 'cpp', install: false)
	test('proto_table_test_' + t, e)
endforeach
