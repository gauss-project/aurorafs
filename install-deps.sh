#!/bin/sh

SHELL_FOLDER=$(pwd)
echo "$SHELL_FOLDER"
mkdir -p "$SHELL_FOLDER"/lib

cd "$SHELL_FOLDER"/lib

if compgen -G "$SHELL_FOLDER"/lib/lib/libsnappy.* > /dev/null; then
  echo "snappy installed"
else
  if [ ! -d "$SHELL_FOLDER"/lib/snappy ]; then
      git clone https://github.com/google/snappy.git -b 1.1.9
      cd snappy
      git submodule update --init
    fi

    cd "$SHELL_FOLDER"/lib/snappy
    [ ! -d build ] && mkdir build
    cd build
    cmake -DCMAKE_INSTALL_PREFIX="$SHELL_FOLDER"/lib ../ || exit
    make || exit
    make install || exit
    cd "$SHELL_FOLDER"/lib
fi

if compgen -G "$SHELL_FOLDER"/lib/lib/libwiredtiger.* > /dev/null; then
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
  ./configure --enable-snappy --prefix="$SHELL_FOLDER"/lib CPPFLAGS="-I$SHELL_FOLDER/lib/include" LDFLAGS="-L$SHELL_FOLDER/lib/include"
  make && make install
fi