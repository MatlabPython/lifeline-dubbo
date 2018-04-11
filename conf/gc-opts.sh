#!/usr/bin/env bash
#
# Runs to configure GC options for specified component.
#
# Command line arguments
#   component - Name of the component to be configured. [master, regionserver, shell]
#   profile - Type of cluster(low/medium/high). By default medium.
#   gc-log-dir - Where log files are stored. By default /tmp.


component=$1
gclogdir=${2:-/tmp}
datetime=$(date "+%Y%m%d%H%M%S")
gclogpath=$gclogdir/$component-$USER-$datetime-%p-gc.log
profile=$3

function gcLogCleanup()
{
  MAX_LOG_FILE=$2
  GC_LOG_PATTERN=$1;
  count=`(ls ${gclogdir}/$GC_LOG_PATTERN | wc -l) 2>/dev/null`
  echo "Gc log files count $count"
  if [ $count -gt $MAX_LOG_FILE ];then
     needToDelete=`expr $count - $MAX_LOG_FILE`
     echo "need to delete $needToDelete files."
     deleted=0
     files=`(ls -rt ${gclogdir}/$GC_LOG_PATTERN) 2>/dev/null`
     for f in $files;
      do
        if [ $deleted -lt $needToDelete ];then
          echo "delete gc log file $f"
          rm -rf $f 2>/dev/null
          deleted=`expr $deleted + 1`
        fi
     done
  fi  
}
MAX_LOG_FILE_CONFIGURATED_FROM_UI='19'
GC_LOG_PATTERN='*-*-*-*-gc.log*';
gcLogCleanup  $GC_LOG_PATTERN $MAX_LOG_FILE_CONFIGURATED_FROM_UI

if [ -z "$profile" ]; then
  echo "No profile is given. Defaults to medium." >&2
  profile=medium
fi

##############################
# Hbase - Master - HIGH
##############################
if [ "$component" = "master" -a "$profile" = "high" ]; then
  GC_OPTS="-server" 
  GC_OPTS="$GC_OPTS -Xms3G" 
  [ "x$HBASE_HEAPSIZE" = "x" ] && GC_OPTS="$GC_OPTS -Xmx3G"
  GC_OPTS="$GC_OPTS -XX:NewSize=256M" 
  GC_OPTS="$GC_OPTS -XX:MaxNewSize=256M" 
  GC_OPTS="$GC_OPTS -XX:MetaspaceSize=128M" 
  GC_OPTS="$GC_OPTS -XX:MaxMetaspaceSize=512M" 
  GC_OPTS="$GC_OPTS -XX:CMSFullGCsBeforeCompaction=1" 
  GC_OPTS="$GC_OPTS -XX:MaxDirectMemorySize=512M" 
  GC_OPTS="$GC_OPTS -XX:+UseConcMarkSweepGC" 
  GC_OPTS="$GC_OPTS -XX:+CMSParallelRemarkEnabled" 
  GC_OPTS="$GC_OPTS -XX:+UseCMSCompactAtFullCollection" 
  GC_OPTS="$GC_OPTS -XX:CMSInitiatingOccupancyFraction=65" 
  GC_OPTS="$GC_OPTS -Xloggc:$gclogpath"
  GC_OPTS="$GC_OPTS -XX:+PrintGCDetails"
  GC_OPTS="$GC_OPTS -Dsun.rmi.dgc.client.gcInterval=0x7FFFFFFFFFFFFFE"
  GC_OPTS="$GC_OPTS -Dsun.rmi.dgc.server.gcInterval=0x7FFFFFFFFFFFFFE"
  GC_OPTS="$GC_OPTS -XX:-OmitStackTraceInFastThrow"
  GC_OPTS="$GC_OPTS -XX:+PrintGCTimeStamps"
  GC_OPTS="$GC_OPTS -XX:+PrintGCDateStamps"
  GC_OPTS="$GC_OPTS -XX:+UseGCLogFileRotation"
  GC_OPTS="$GC_OPTS -XX:NumberOfGCLogFiles=10"
  GC_OPTS="$GC_OPTS -XX:GCLogFileSize=1M"
#  GC_OPTS="$GC_OPTS -XX:+DisableExplicitGC"
fi

##############################
# Hbase - RegionServer - HIGH
##############################
if [ "$component" = "regionserver" -a "$profile" = "high" ]; then
  GC_OPTS="-server" 
  GC_OPTS="$GC_OPTS -Xms10G" 
  [ "x$HBASE_HEAPSIZE" = "x" ] && GC_OPTS="$GC_OPTS -Xmx10G"
  GC_OPTS="$GC_OPTS -XX:NewSize=512M" 
  GC_OPTS="$GC_OPTS -XX:MaxNewSize=512M" 
  GC_OPTS="$GC_OPTS -XX:MetaspaceSize=128M" 
  GC_OPTS="$GC_OPTS -XX:MaxMetaspaceSize=512M" 
  GC_OPTS="$GC_OPTS -XX:CMSFullGCsBeforeCompaction=1" 
  GC_OPTS="$GC_OPTS -XX:MaxDirectMemorySize=1G"
  GC_OPTS="$GC_OPTS -XX:+UseConcMarkSweepGC" 
  GC_OPTS="$GC_OPTS -XX:+CMSParallelRemarkEnabled" 
  GC_OPTS="$GC_OPTS -XX:+UseCMSCompactAtFullCollection" 
  GC_OPTS="$GC_OPTS -XX:CMSInitiatingOccupancyFraction=65" 
  GC_OPTS="$GC_OPTS -Xloggc:$gclogpath"
  GC_OPTS="$GC_OPTS -XX:+PrintGCDetails"
  GC_OPTS="$GC_OPTS -Dsun.rmi.dgc.client.gcInterval=0x7FFFFFFFFFFFFFE"
  GC_OPTS="$GC_OPTS -Dsun.rmi.dgc.server.gcInterval=0x7FFFFFFFFFFFFFE"
  GC_OPTS="$GC_OPTS -XX:-OmitStackTraceInFastThrow"
  GC_OPTS="$GC_OPTS -XX:+PrintGCTimeStamps"
  GC_OPTS="$GC_OPTS -XX:+PrintGCDateStamps"
  GC_OPTS="$GC_OPTS -XX:+UseGCLogFileRotation"
  GC_OPTS="$GC_OPTS -XX:NumberOfGCLogFiles=10"
  GC_OPTS="$GC_OPTS -XX:GCLogFileSize=1M"
#  GC_OPTS="$GC_OPTS -XX:+DisableExplicitGC"
fi

##############################
# Hbase - Client - HIGH
##############################
if [ "$component" = "shell" -a "$profile" = "high" ]; then
  GC_OPTS="-client"
  GC_OPTS="$GC_OPTS -Xms512M"
  [ "x$HBASE_HEAPSIZE" = "x" ] && GC_OPTS="$GC_OPTS -Xmx512M"
  GC_OPTS="$GC_OPTS -XX:+PrintGCTimeStamps"
  GC_OPTS="$GC_OPTS -XX:+PrintGCDateStamps"
  GC_OPTS="$GC_OPTS -XX:NewSize=64M" 
  GC_OPTS="$GC_OPTS -XX:MaxNewSize=64M" 
  GC_OPTS="$GC_OPTS -XX:MetaspaceSize=64M" 
  GC_OPTS="$GC_OPTS -XX:MaxMetaspaceSize=64M"
fi

##############################
# Hbase - Thrift/Thrift2 - HIGH
##############################
if [ "$component" = "thrift" -o "$component" = "thrift2" ] && [ "$profile" = "high" ]; then
  GC_OPTS="-server" 
  GC_OPTS="$GC_OPTS -Xms2G" 
  [ "x$HBASE_HEAPSIZE" = "x" ] && GC_OPTS="$GC_OPTS -Xmx2G"
  GC_OPTS="$GC_OPTS -XX:NewSize=128M" 
  GC_OPTS="$GC_OPTS -XX:MaxNewSize=128M" 
  GC_OPTS="$GC_OPTS -XX:MetaspaceSize=128M" 
  GC_OPTS="$GC_OPTS -XX:MaxMetaspaceSize=512M" 
  GC_OPTS="$GC_OPTS -XX:CMSFullGCsBeforeCompaction=1" 
  GC_OPTS="$GC_OPTS -XX:MaxDirectMemorySize=128M"
  GC_OPTS="$GC_OPTS -XX:+UseConcMarkSweepGC" 
  GC_OPTS="$GC_OPTS -XX:+CMSParallelRemarkEnabled" 
  GC_OPTS="$GC_OPTS -XX:+UseCMSCompactAtFullCollection" 
  GC_OPTS="$GC_OPTS -XX:CMSInitiatingOccupancyFraction=65" 
  GC_OPTS="$GC_OPTS -Xloggc:$gclogpath"
  GC_OPTS="$GC_OPTS -XX:+PrintGCDetails"
  GC_OPTS="$GC_OPTS -Dsun.rmi.dgc.client.gcInterval=0x7FFFFFFFFFFFFFE"
  GC_OPTS="$GC_OPTS -Dsun.rmi.dgc.server.gcInterval=0x7FFFFFFFFFFFFFE"
  GC_OPTS="$GC_OPTS -XX:-OmitStackTraceInFastThrow"
  GC_OPTS="$GC_OPTS -XX:+PrintGCTimeStamps"
  GC_OPTS="$GC_OPTS -XX:+PrintGCDateStamps"
  GC_OPTS="$GC_OPTS -XX:+UseGCLogFileRotation"
  GC_OPTS="$GC_OPTS -XX:NumberOfGCLogFiles=10"
  GC_OPTS="$GC_OPTS -XX:GCLogFileSize=1M"
#  GC_OPTS="$GC_OPTS -XX:+DisableExplicitGC"
fi

##############################
# Hbase - Master - MEDIUM
##############################
if [ "$component" = "master" -a "$profile" = "medium" ]; then
  GC_OPTS="-server" 
  GC_OPTS="$GC_OPTS -Xms2G" 
  [ "x$HBASE_HEAPSIZE" = "x" ] && GC_OPTS="$GC_OPTS -Xmx2G"
  GC_OPTS="$GC_OPTS -XX:NewSize=256M" 
  GC_OPTS="$GC_OPTS -XX:MaxNewSize=256M" 
  GC_OPTS="$GC_OPTS -XX:MetaspaceSize=128M" 
  GC_OPTS="$GC_OPTS -XX:MaxMetaspaceSize=512M" 
  GC_OPTS="$GC_OPTS -XX:CMSFullGCsBeforeCompaction=1" 
  GC_OPTS="$GC_OPTS -XX:MaxDirectMemorySize=512M"
  GC_OPTS="$GC_OPTS -XX:+UseConcMarkSweepGC" 
  GC_OPTS="$GC_OPTS -XX:+CMSParallelRemarkEnabled" 
  GC_OPTS="$GC_OPTS -XX:+UseCMSCompactAtFullCollection" 
  GC_OPTS="$GC_OPTS -XX:CMSInitiatingOccupancyFraction=65" 
  GC_OPTS="$GC_OPTS -Xloggc:$gclogpath"
  GC_OPTS="$GC_OPTS -XX:+PrintGCDetails"
  GC_OPTS="$GC_OPTS -Dsun.rmi.dgc.client.gcInterval=0x7FFFFFFFFFFFFFE"
  GC_OPTS="$GC_OPTS -Dsun.rmi.dgc.server.gcInterval=0x7FFFFFFFFFFFFFE"
  GC_OPTS="$GC_OPTS -XX:-OmitStackTraceInFastThrow"
  GC_OPTS="$GC_OPTS -XX:+PrintGCTimeStamps"
  GC_OPTS="$GC_OPTS -XX:+PrintGCDateStamps"
  GC_OPTS="$GC_OPTS -XX:+UseGCLogFileRotation"
  GC_OPTS="$GC_OPTS -XX:NumberOfGCLogFiles=10"
  GC_OPTS="$GC_OPTS -XX:GCLogFileSize=1M"
#  GC_OPTS="$GC_OPTS -XX:+DisableExplicitGC"
fi

##############################
# Hbase - RegionServer - MEDIUM
##############################
if [ "$component" = "regionserver" -a "$profile" = "medium" ]; then
  GC_OPTS="-server" 
  GC_OPTS="$GC_OPTS -Xms8G" 
  [ "x$HBASE_HEAPSIZE" = "x" ] && GC_OPTS="$GC_OPTS -Xmx8G"
  GC_OPTS="$GC_OPTS -XX:NewSize=512M" 
  GC_OPTS="$GC_OPTS -XX:MaxNewSize=512M" 
  GC_OPTS="$GC_OPTS -XX:MetaspaceSize=128M" 
  GC_OPTS="$GC_OPTS -XX:MaxMetaspaceSize=512M" 
  GC_OPTS="$GC_OPTS -XX:CMSFullGCsBeforeCompaction=1" 
  GC_OPTS="$GC_OPTS -XX:MaxDirectMemorySize=512M"
  GC_OPTS="$GC_OPTS -XX:+UseConcMarkSweepGC" 
  GC_OPTS="$GC_OPTS -XX:+CMSParallelRemarkEnabled" 
  GC_OPTS="$GC_OPTS -XX:+UseCMSCompactAtFullCollection" 
  GC_OPTS="$GC_OPTS -XX:CMSInitiatingOccupancyFraction=65" 
  GC_OPTS="$GC_OPTS -Xloggc:$gclogpath"
  GC_OPTS="$GC_OPTS -XX:+PrintGCDetails"
  GC_OPTS="$GC_OPTS -Dsun.rmi.dgc.client.gcInterval=0x7FFFFFFFFFFFFFE"
  GC_OPTS="$GC_OPTS -Dsun.rmi.dgc.server.gcInterval=0x7FFFFFFFFFFFFFE"
  GC_OPTS="$GC_OPTS -XX:-OmitStackTraceInFastThrow"
  GC_OPTS="$GC_OPTS -XX:+PrintGCTimeStamps"
  GC_OPTS="$GC_OPTS -XX:+PrintGCDateStamps"
  GC_OPTS="$GC_OPTS -XX:+UseGCLogFileRotation"
  GC_OPTS="$GC_OPTS -XX:NumberOfGCLogFiles=10"
  GC_OPTS="$GC_OPTS -XX:GCLogFileSize=1M"
#  GC_OPTS="$GC_OPTS -XX:+DisableExplicitGC"
fi

##############################
# Hbase - Client - MEDIUM
##############################
if [ "$component" = "shell" -a "$profile" = "medium" ]; then
  GC_OPTS="-client"
  GC_OPTS="$GC_OPTS -Xms256M"
  [ "x$HBASE_HEAPSIZE" = "x" ] && GC_OPTS="$GC_OPTS -Xmx256M"
  GC_OPTS="$GC_OPTS -XX:+PrintGCTimeStamps"
  GC_OPTS="$GC_OPTS -XX:+PrintGCDateStamps"
  GC_OPTS="$GC_OPTS -XX:NewSize=64M" 
  GC_OPTS="$GC_OPTS -XX:MaxNewSize=64M" 
  GC_OPTS="$GC_OPTS -XX:MetaspaceSize=64M" 
  GC_OPTS="$GC_OPTS -XX:MaxMetaspaceSize=64M"
fi

##############################
# Hbase - Thrift/Thrift2 - MEDIUM
##############################
if [ "$component" = "thrift" -o "$component" = "thrift2" ] && [ "$profile" = "medium" ]; then
  GC_OPTS="-server" 
  GC_OPTS="$GC_OPTS -Xms1G" 
  [ "x$HBASE_HEAPSIZE" = "x" ] && GC_OPTS="$GC_OPTS -Xmx1G"
  GC_OPTS="$GC_OPTS -XX:NewSize=128M" 
  GC_OPTS="$GC_OPTS -XX:MaxNewSize=128M" 
  GC_OPTS="$GC_OPTS -XX:MetaspaceSize=128M" 
  GC_OPTS="$GC_OPTS -XX:MaxMetaspaceSize=512M" 
  GC_OPTS="$GC_OPTS -XX:CMSFullGCsBeforeCompaction=1" 
  GC_OPTS="$GC_OPTS -XX:MaxDirectMemorySize=128M"
  GC_OPTS="$GC_OPTS -XX:+UseConcMarkSweepGC" 
  GC_OPTS="$GC_OPTS -XX:+CMSParallelRemarkEnabled" 
  GC_OPTS="$GC_OPTS -XX:+UseCMSCompactAtFullCollection" 
  GC_OPTS="$GC_OPTS -XX:CMSInitiatingOccupancyFraction=65" 
  GC_OPTS="$GC_OPTS -Xloggc:$gclogpath"
  GC_OPTS="$GC_OPTS -XX:+PrintGCDetails"
  GC_OPTS="$GC_OPTS -Dsun.rmi.dgc.client.gcInterval=0x7FFFFFFFFFFFFFE"
  GC_OPTS="$GC_OPTS -Dsun.rmi.dgc.server.gcInterval=0x7FFFFFFFFFFFFFE"
  GC_OPTS="$GC_OPTS -XX:-OmitStackTraceInFastThrow"
  GC_OPTS="$GC_OPTS -XX:+PrintGCTimeStamps"
  GC_OPTS="$GC_OPTS -XX:+PrintGCDateStamps"
  GC_OPTS="$GC_OPTS -XX:+UseGCLogFileRotation"
  GC_OPTS="$GC_OPTS -XX:NumberOfGCLogFiles=10"
  GC_OPTS="$GC_OPTS -XX:GCLogFileSize=1M"
#  GC_OPTS="$GC_OPTS -XX:+DisableExplicitGC"
fi

##############################
# Hbase - Master - LOW
##############################
if [ "$component" = "master" -a "$profile" = "low" ]; then
  GC_OPTS="-server" 
  GC_OPTS="$GC_OPTS -Xms1G" 
  [ "x$HBASE_HEAPSIZE" = "x" ] && GC_OPTS="$GC_OPTS -Xmx1G"
  GC_OPTS="$GC_OPTS -XX:NewSize=64M" 
  GC_OPTS="$GC_OPTS -XX:MaxNewSize=64M" 
  GC_OPTS="$GC_OPTS -XX:MetaspaceSize=128M" 
  GC_OPTS="$GC_OPTS -XX:MaxMetaspaceSize=512M"
  GC_OPTS="$GC_OPTS -XX:MaxDirectMemorySize=512M"
  GC_OPTS="$GC_OPTS -Dsun.rmi.dgc.client.gcInterval=0x7FFFFFFFFFFFFFE"
  GC_OPTS="$GC_OPTS -Dsun.rmi.dgc.server.gcInterval=0x7FFFFFFFFFFFFFE"
  GC_OPTS="$GC_OPTS -Xloggc:$gclogpath"
  GC_OPTS="$GC_OPTS -XX:+PrintGCTimeStamps"
  GC_OPTS="$GC_OPTS -XX:+PrintGCDateStamps"
  GC_OPTS="$GC_OPTS -XX:+UseGCLogFileRotation"
  GC_OPTS="$GC_OPTS -XX:NumberOfGCLogFiles=10"
  GC_OPTS="$GC_OPTS -XX:GCLogFileSize=1M"
fi

##############################
# Hbase - RegionServer - LOW
##############################
if [ "$component" = "regionserver" -a "$profile" = "low" ]; then
  GC_OPTS="-server" 
  GC_OPTS="$GC_OPTS -Xms2G" 
  [ "x$HBASE_HEAPSIZE" = "x" ] && GC_OPTS="$GC_OPTS -Xmx2G"
  GC_OPTS="$GC_OPTS -XX:NewSize=128M" 
  GC_OPTS="$GC_OPTS -XX:MaxNewSize=128M" 
  GC_OPTS="$GC_OPTS -XX:MetaspaceSize=128M" 
  GC_OPTS="$GC_OPTS -XX:MaxMetaspaceSize=512M"
  GC_OPTS="$GC_OPTS -XX:MaxDirectMemorySize=512M"
  GC_OPTS="$GC_OPTS -Dsun.rmi.dgc.client.gcInterval=0x7FFFFFFFFFFFFFE"
  GC_OPTS="$GC_OPTS -Dsun.rmi.dgc.server.gcInterval=0x7FFFFFFFFFFFFFE"
  GC_OPTS="$GC_OPTS -Xloggc:$gclogpath"
  GC_OPTS="$GC_OPTS -XX:+PrintGCTimeStamps"
  GC_OPTS="$GC_OPTS -XX:+PrintGCDateStamps"
  GC_OPTS="$GC_OPTS -XX:+UseGCLogFileRotation"
  GC_OPTS="$GC_OPTS -XX:NumberOfGCLogFiles=10"
  GC_OPTS="$GC_OPTS -XX:GCLogFileSize=1M"
fi

##############################
# Hbase - Client - LOW
##############################
if [ "$component" = "shell" -a "$profile" = "low" ]; then
  GC_OPTS="-client"
  GC_OPTS="$GC_OPTS -Xms128M"
  [ "x$HBASE_HEAPSIZE" = "x" ] && GC_OPTS="$GC_OPTS -Xmx128M"
  GC_OPTS="$GC_OPTS -XX:NewSize=64M" 
  GC_OPTS="$GC_OPTS -XX:MaxNewSize=64M" 
  GC_OPTS="$GC_OPTS -XX:MetaspaceSize=64M" 
  GC_OPTS="$GC_OPTS -XX:MaxMetaspaceSize=64M"
  GC_OPTS="$GC_OPTS -XX:+PrintGCTimeStamps"
  GC_OPTS="$GC_OPTS -XX:+PrintGCDateStamps"
fi

##############################
# Hbase - Thrift/Thrift2 - LOW
##############################
if [ "$component" = "thrift" -o "$component" = "thrift2" ] && [ "$profile" = "low" ]; then
  GC_OPTS="-server" 
  GC_OPTS="$GC_OPTS -Xms512M" 
  [ "x$HBASE_HEAPSIZE" = "x" ] && GC_OPTS="$GC_OPTS -Xmx512M"
  GC_OPTS="$GC_OPTS -XX:NewSize=64M" 
  GC_OPTS="$GC_OPTS -XX:MaxNewSize=64M" 
  GC_OPTS="$GC_OPTS -XX:MetaspaceSize=128M" 
  GC_OPTS="$GC_OPTS -XX:MaxMetaspaceSize=512M"
  GC_OPTS="$GC_OPTS -XX:MaxDirectMemorySize=128M"
  GC_OPTS="$GC_OPTS -Dsun.rmi.dgc.client.gcInterval=0x7FFFFFFFFFFFFFE"
  GC_OPTS="$GC_OPTS -Dsun.rmi.dgc.server.gcInterval=0x7FFFFFFFFFFFFFE"
  GC_OPTS="$GC_OPTS -Xloggc:$gclogpath"
  GC_OPTS="$GC_OPTS -XX:+PrintGCTimeStamps"
  GC_OPTS="$GC_OPTS -XX:+PrintGCDateStamps"
  GC_OPTS="$GC_OPTS -XX:+UseGCLogFileRotation"
  GC_OPTS="$GC_OPTS -XX:NumberOfGCLogFiles=10"
  GC_OPTS="$GC_OPTS -XX:GCLogFileSize=1M"
fi

if [ "$profile" = "custom"  ]; then
  GC_OPTS="$GC_OPTS -Xloggc:$gclogpath"
  echo "$GC_OPTS" | sed s/"-Xmx.*-XX:NewSize"/"-XX:NewSize"/g > $gclogdir/tmp.out
  [ "x$HBASE_HEAPSIZE" != "x" ] && GC_OPTS=`cat $gclogdir/tmp.out`
fi

if [ -e "$gclogdir/tmp.out" ]; then
    rm -rf /$gclogdir/tmp.out
fi

export HBASE_OPTS="$HBASE_OPTS $GC_OPTS -Djava.io.tmpdir=$BIGDATA_DATA_HOME/tmp/snappy_${PID_FILE_NAME_PREFIX} -Dorg.xerial.snappy.tempdir=$BIGDATA_DATA_HOME/tmp/snappy_${PID_FILE_NAME_PREFIX} -Dbeetle.application.home.path=$WCC_PROFILE_DIR"
