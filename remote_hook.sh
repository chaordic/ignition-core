#!/bin/bash


# We suppose we are in a subdirectory of the root project
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

JOB_NAME="${1:?Please give a Job Name}"
JOB_DATE="${2?Please give the Job Date}"
JOB_TAG="${3?Please give the Job Tag}"
JOB_USER="${4?Please give the Job User}"
CONTROL_DIR="${5?Please give the Control Directory}"
SPARK_MEM_PARAM="${6?Please give the Job Memory Size to use}"
USE_YARN="${7?Please tell if we should use YARN (yes/no)}"
NOTIFY_ON_ERRORS="${8?Please tell if we will notify on errors (yes/no)}"
DRIVER_HEAP_SIZE="${9?Please tell driver heap size to use}"
shift 9

JOB_WITH_TAG=${JOB_NAME}.${JOB_TAG}
JOB_CONTROL_DIR="${CONTROL_DIR}/${JOB_WITH_TAG}"
MY_USER=$(whoami)

# Avoids problems when another user created our control dir
sudo mkdir -p "${JOB_CONTROL_DIR}"
sudo chown $MY_USER "${JOB_CONTROL_DIR}"

RUNNING_FILE="${JOB_CONTROL_DIR}/RUNNING"
# This should be the first thing in the script to avoid the wait remote job thinking we died
echo $$ > "${RUNNING_FILE}"



# Let us read the spark home even when the image doesn't give us the permission
sudo chmod o+rx /root
sudo chmod -R o+rx /root/spark

sudo mkdir -p /media/tmp/spark-events

notify_error_and_exit() {
    description="${1}"
    echo "Exiting because: ${description}"
    echo "${description}" > "${JOB_CONTROL_DIR}/FAILURE"
    # Do not notify if NOTIFY_ON_ERRORS is different from yes
    [[ "${NOTIFY_ON_ERRORS}" != 'yes' ]] && exit 1
    # TODO: create some generic method to notify on errors
    exit 1
}

get_first_present() {
    for dir in "$@"; do
        if [[ -e "$dir" ]]; then
            echo $dir
            break
        fi
    done
}

on_trap_exit() {
    rm -f "${JOB_CONTROL_DIR}"/*.jar
    rm -f "${RUNNING_FILE}"
}

install_and_run_zeppelin() {
    if [[ ! -d "zeppelin" ]]; then
        wget "http://www-us.apache.org/dist/zeppelin/zeppelin-0.8.0/zeppelin-0.8.0-bin-all.tgz" -O zeppelin.tar.gz
        mkdir zeppelin
        tar xvzf zeppelin.tar.gz -C zeppelin --strip-components 1 > /tmp/zeppelin_install.log
    fi
    if [[ -f "zeppelin/bin/zeppelin.sh" ]]; then
        export MASTER="${JOB_MASTER}"
        export ZEPPELIN_PORT="8081"
        export SPARK_HOME=$(get_first_present /root/spark /opt/spark ~/spark*/)
        export SPARK_SUBMIT_OPTIONS="--jars ${JAR_PATH} --executor-memory ${SPARK_MEM_PARAM}"
        sudo -E zeppelin/bin/zeppelin.sh
    else
        notify_error_and_exit "Zeppelin installation not found"
    fi
}

install_and_run_jupyter() {
    sudo yum -y install python3 python3-pip
    sudo pip3 install jupyter pandas boto3 matplotlib numpy sklearn scipy toree
    export SPARK_HOME=$(get_first_present /root/spark /opt/spark ~/spark*/)
    export HADOOP_HOME=$(get_first_present /root/hadoop /opt/hadoop ~/hadoop*/)
    export SPARK_CONF_DIR="${SPARK_HOME}/conf"
    export HADOOP_CONF_DIR="${HADOOP_HOME}/conf"
    export JOB_MASTER=${MASTER:-spark://${SPARK_MASTER_HOST}:7077}
    export PYSPARK_PYTHON=$(which python3)
    export PYSPARK_DRIVER_PYTHON=$(which jupyter)
    export PYSPARK_DRIVER_PYTHON_OPTS="notebook --allow-root --ip=${SPARK_MASTER_HOST} --no-browser --port=8888"
    sudo $(which jupyter) toree install --spark_home="${SPARK_HOME}" --spark_opts="--master ${JOB_MASTER} --executor-memory ${SPARK_MEM_PARAM} --driver-memory ${DRIVER_HEAP_SIZE}"
    sudo -E "${SPARK_HOME}/bin/pyspark" --master "${JOB_MASTER}" --executor-memory "${SPARK_MEM_PARAM}" --driver-memory "${DRIVER_HEAP_SIZE}"
}

trap "on_trap_exit" EXIT

SPARK_HOME=$(get_first_present /root/spark /opt/spark ~/spark*/)
source "${SPARK_HOME}/conf/spark-env.sh"

MAIN_CLASS="ignition.jobs.Runner"

cd "${DIR}" || notify_error_and_exit "Internal script error for job ${JOB_WITH_TAG}"

JAR_PATH_SRC=$(ls -t "${DIR}"/*assembly*.jar | head -1) # most recent jar
JAR_PATH="${JOB_CONTROL_DIR}/Ignition.jar"

cp ${JAR_PATH_SRC} ${JAR_PATH}

# If no $MASTER, then build a url using $SPARK_MASTER_HOST
export JOB_MASTER=${MASTER:-spark://${SPARK_MASTER_HOST}:7077}


if [[ "${USE_YARN}" == "yes" ]]; then
    export YARN_MODE=true
    export JOB_MASTER="yarn-client"
    export SPARK_JAR=$(echo ${SPARK_HOME}/assembly/target/scala-*/spark-assembly-*.jar)
    export SPARK_YARN_APP_JAR=${JAR_PATH}
    export SPARK_WORKER_MEMORY=${SPARK_MEM_PARAM}
fi

if [[ "${JOB_NAME}" == "shell" ]]; then
    sudo -E ${SPARK_HOME}/bin/spark-shell --master "${JOB_MASTER}" --jars ${JAR_PATH} --driver-memory "${DRIVER_HEAP_SIZE}" --driver-java-options "-Djava.io.tmpdir=/media/tmp -verbose:gc -XX:-PrintGCDetails -XX:+PrintGCTimeStamps" --executor-memory "${SPARK_MEM_PARAM}" || notify_error_and_exit "Execution failed for shell"
elif [[ "${JOB_NAME}" == "zeppelin" ]]; then
    install_and_run_zeppelin
elif [[ "${JOB_NAME}" == "jupyter" ]]; then
    install_and_run_jupyter
else
    JOB_OUTPUT="${JOB_CONTROL_DIR}/output.log"
    tail -F "${JOB_OUTPUT}" &
    sudo -E "${SPARK_HOME}/bin/spark-submit" --master "${JOB_MASTER}" --driver-memory "${DRIVER_HEAP_SIZE}" --driver-java-options "-Djava.io.tmpdir=/media/tmp -verbose:gc -XX:-PrintGCDetails -XX:+PrintGCTimeStamps" --class "${MAIN_CLASS}" ${JAR_PATH} "${JOB_NAME}" --runner-date "${JOB_DATE}" --runner-tag "${JOB_TAG}" --runner-user "${JOB_USER}" --runner-master "${JOB_MASTER}" --runner-executor-memory "${SPARK_MEM_PARAM}" "$@" >& "${JOB_OUTPUT}" || notify_error_and_exit "Execution failed for job ${JOB_WITH_TAG}"
fi

touch "${JOB_CONTROL_DIR}/SUCCESS"
