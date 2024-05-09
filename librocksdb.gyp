{
  'targets': [{
    'target_name': 'librocksdb',
    'type': 'static_library',
    'include_dirs': [
      './include',
    ],
    'dependencies': [
      './vendor/rocksdb.gyp:rocksdb',
    ],
    'sources': [
      './src/rocksdb.cc',
    ],
    'direct_dependent_settings': {
      'include_dirs': [
        './include',
      ],
    },
    'export_dependent_settings': [
      './vendor/rocksdb.gyp:rocksdb',
    ],
    'configurations': {
      'Debug': {
        'defines': ['DEBUG'],
      },
      'Release': {
        'defines': ['NDEBUG'],
      },
    },
  }]
}
