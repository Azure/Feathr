#!/bin/sh

set -o errexit

cargo build

kill() {
    if [ "$(uname)" = "Darwin" ]; then
        SERVICE='registry-app'
        if pgrep -xq -- "${SERVICE}"; then
            pkill -f "${SERVICE}"
        fi
    else
        set +e # killall will error if finds no process to kill
        killall registry-app
        set -e
    fi
}

if [ "xxx${RAFT_MANAGEMENT_CODE}" == "xxx" ]; then
    export MGMT_HDR="a: b"
else
    export MGMT_HDR="x-registry-management-code: ${RAFT_MANAGEMENT_CODE}"
fi


rpc() {
    local uri=$1
    local body="$2"

    echo '---'" rpc(:$uri, $body)"

    {
        if [ ".$body" = "." ]; then
            curl -H"${MGMT_HDR}" --silent "127.0.0.1:$uri"
        else
            curl -H"${MGMT_HDR}" --silent "127.0.0.1:$uri" -H "Content-Type: application/json" -d "$body"
        fi
    } | {
        if type jq > /dev/null 2>&1; then
            jq
        else
            cat
        fi
    }

    echo
    echo
}

echo "Killing all running registry-app"

kill

sleep 1

echo "Start 3 uninitialized registry-app servers..."

nohup ./target/debug/feathr-registry  --node-id 1 --http-addr 127.0.0.1:21001 > test-n1.log &
sleep 1
echo "Server 1 started"

nohup ./target/debug/feathr-registry  --node-id 2 --http-addr 127.0.0.1:21002 > test-n2.log &
sleep 1
echo "Server 2 started"

nohup ./target/debug/feathr-registry  --node-id 3 --http-addr 127.0.0.1:21003 > test-n3.log &
sleep 1
echo "Server 3 started"
sleep 1

echo "Initialize server 1 as a single-node cluster"
sleep 2
echo
rpc 21001/init '{}'

echo "Server 1 is a leader now"

sleep 2

echo "Get metrics from the leader"
sleep 2
echo
rpc 21001/metrics
sleep 1

echo "Write data on leader"
sleep 1
echo
rpc 21001/api/projects '{"name": "project0"}'
sleep 1
echo "Data written"
sleep 1


echo "Adding node 2 and node 3 as learners, to receive log from leader node 1"

sleep 1
echo
rpc 21001/add-learner       '[2, "127.0.0.1:21002"]'
echo "Node 2 added as leaner"
sleep 1
echo
rpc 21001/add-learner       '[3, "127.0.0.1:21003"]'
echo "Node 3 added as leaner"
sleep 1

echo "Get metrics from the leader, after adding 2 learners"
sleep 2
echo
rpc 21001/metrics
sleep 1

echo "Changing membership from [1] to 3 nodes cluster: [1, 2, 3]"
echo
rpc 21001/change-membership '[1, 2, 3]'
sleep 1
echo "Membership changed"
sleep 1

echo "Get metrics from the leader again"
sleep 1
echo
rpc 21001/metrics
sleep 1

echo "Write data on leader"
sleep 1
echo
rpc 21001/api/projects '{"name": "project1"}'
sleep 1
echo "Data written"
sleep 1

echo "Read on every node, including the leader"
sleep 1
echo "Read from node 1"
echo
rpc 21001/api/projects
echo "Read from node 2"
echo
rpc 21002/api/projects
echo "Read from node 3"
echo
rpc 21003/api/projects

##############################################################{
echo "Write data on leader"
sleep 1
echo
rpc 21001/api/projects '{"name": "project2"}'

sleep 1
echo "Data written"
sleep 1

echo "Read on every node, including the leader"
sleep 1
echo "Read from node 1"
echo
rpc 21001/api/projects
echo "Read from node 2"
echo
rpc 21002/api/projects
echo "Read from node 3"
echo
rpc 21003/api/projects
##############################################################}

echo "Killing all nodes in 3s..."
sleep 1
echo "Killing all nodes in 2s..."
sleep 1
echo "Killing all nodes in 1s..."
sleep 1
# kill
