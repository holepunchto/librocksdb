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
    'conditions': [
      ['OS=="mac"', {
        'xcode_settings': {
          'OTHER_CPLUSPLUSFLAGS': [
            '-std=c++20',
          ],
        },
      }],
      ['OS=="linux"', {
        'cflags_cc': [
          '-std=c++20',
        ],
      }],
      ['OS=="win"', {
        'cflags_cc': [
          '/std:c++20',
        ],
      }],
    ],
  }]
}
