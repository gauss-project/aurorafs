#!/bin/sh

SHELL_FOLDER="$(pwd)"
INSTALL_DIR="${1:-$SHELL_FOLDER/lib}"
IS_DOCKER="${2:-false}"

mkdir -p "$SHELL_FOLDER"/lib
cd "$SHELL_FOLDER"/lib

if bash -c "compgen -G "$INSTALL_DIR"/lib/libsnappy.* > /dev/null"; then
  echo "snappy installed"
else
  if [ ! -d "$SHELL_FOLDER"/lib/snappy ]; then
      git clone https://github.com/google/snappy.git -b 1.1.9
      cd snappy
      git submodule update --init --recursive
      cat <<"EOF" > snappy.pc.in
      prefix=@CMAKE_INSTALL_PREFIX@
      exec_prefix=${prefix}
      libdir=${prefix}/lib
      includedir=${prefix}/include

      Name: snappy
      Description: Fast compressor/decompressor library.
      Version: @PROJECT_VERSION@
      Libs: -L${libdir} -lsnappy
      Cflags: -I${includedir}
EOF
      cat <<"EOF" > cmake_add_pkgconfig.patch
      --- a/CMakeLists.txt
      +++ b/CMakeLists.txt
      @@ -187,6 +187,12 @@
         "${PROJECT_BINARY_DIR}/config.h"
       )

      +configure_file(
      +  "${CMAKE_CURRENT_SOURCE_DIR}/snappy.pc.in"
      +  "${CMAKE_CURRENT_BINARY_DIR}/snappy.pc"
      +  @ONLY
      +)
      +
       # We don't want to define HAVE_ macros in public headers. Instead, we use
       # CMake's variable substitution with 0/1 variables, which will be seen by the
       # preprocessor as constants.
      @@ -395,4 +401,8 @@
             "${PROJECT_BINARY_DIR}/cmake/${PROJECT_NAME}ConfigVersion.cmake"
           DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}"
         )
      +  install(
      +    FILES "${PROJECT_BINARY_DIR}/snappy.pc"
      +    DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig"
      +  )
       endif(SNAPPY_INSTALL)
EOF
      patch -p1 < cmake_add_pkgconfig.patch
      wget -O reenable_rtti.patch https://github.com/google/snappy/commit/516fdcca6606502e2d562d20c01b225c8d066739.patch || exit
      patch -p1 < reenable_rtti.patch
      wget -O fix_inline.patch https://github.com/google/snappy/pull/128/commits/0c716d435abe65250100c2caea0e5126ac4e14bd.patch || exit
      patch -p1 < fix_inline.patch
    fi

    cd "$SHELL_FOLDER"/lib/snappy
    [ ! -d build ] && mkdir build
    cd build
    cmake -DCMAKE_INSTALL_PREFIX="$INSTALL_DIR" -DCMAKE_INSTALL_LIBDIR="$INSTALL_DIR"/lib -DBUILD_SHARED_LIBS=yes -DSNAPPY_BUILD_TESTS=OFF -DSNAPPY_BUILD_BENCHMARKS=OFF ../ || exit
    make || exit
    make install || exit
    make clean
    cd "$SHELL_FOLDER"/lib
fi

if $IS_DOCKER; then
  cp -rd "$INSTALL_DIR"/lib/* /usr/local/lib
  ldconfig
fi

if bash -c "compgen -G "$INSTALL_DIR"/lib/libwiredtiger.* > /dev/null"; then
  echo "wiredtiger installed"
else
  if [ ! -d "$SHELL_FOLDER"/lib/wiredtiger ]; then
      git clone https://github.com/wiredtiger/wiredtiger.git -b mongodb-5.0
      cd wiredtiger
  else
      cd wiredtiger
      git checkout mongodb-5.0
      git pull https://github.com/wiredtiger/wiredtiger.git
  fi

  cd "$SHELL_FOLDER"/lib/wiredtiger
  sh autogen.sh
  ./configure --enable-snappy --enable-tcmalloc --prefix="$INSTALL_DIR" CPPFLAGS="-I$INSTALL_DIR/include" LDFLAGS="-L$INSTALL_DIR/include" || exit
  make || exit
  make install || exit
  make clean
fi

if $IS_DOCKER; then
  cp -rd "$INSTALL_DIR"/lib/* /usr/local/lib
  cp -rd "$INSTALL_DIR"/include/* /usr/local/include
fi

if command -v ldconfig > /dev/null; then
  ldconfig "$INSTALL_DIR"/lib
fi