#!/bin/bash
if [ $# -lt 1 ];
then
  echo "USAGE: $0 classname [opts]"
  exit 1
fi

home_dir=`pwd`
base_dir=$(dirname $0)/..
cd $base_dir
base_dir=`pwd`
cd $home_dir

echo home_dir=$home_dir
echo "framework base (location of this script). base_dir=$base_dir"

if [ ! -d "$base_dir/lib" ]; then
  echo "Unable to find $base_dir/lib, which is required to run."
  exit 1
fi

DEFAULT_LOG4J_FILE=$base_dir/resources/log4j.xml
BASE_LIB_DIR="$base_dir/lib"


if [ -d "$BASE_LIB_DIR" ]; then
  # build a classpath that includes all generated jars/wars in BASE_LIB_DIR
  for file in $BASE_LIB_DIR/*.[jw]ar;
  do
    CLASSPATH=$CLASSPATH:$file
  done
fi

if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

# add usercache directory
mkdir -p $base_dir/tmp
JAVA_TEMP_DIR=$base_dir/tmp

# Check if log4j configuration is specified. If not - set to resources/log4j.xml
[[ $JAVA_OPTS != *-Dlog4j.configuration* && -f $DEFAULT_LOG4J_FILE ]] && JAVA_OPTS="$JAVA_OPTS -Dlog4j.configuration=file:$DEFAULT_LOG4J_FILE"

# Check if java.io.tmpdir is specified. If not - set to tmp in the base_dir
[[ $JAVA_OPTS != *-Djava.io.tmpdir* ]] && JAVA_OPTS="$JAVA_OPTS -Djava.io.tmpdir=$JAVA_TEMP_DIR"

# Check if a max-heap size is specified. If not - set a 768M heap
[[ $JAVA_OPTS != *-Xmx* ]] && JAVA_OPTS="$JAVA_OPTS -Xmx768M"

echo $JAVA $JAVA_OPTS -cp $CLASSPATH "$@"
exec $JAVA $JAVA_OPTS -cp $CLASSPATH "$@"
