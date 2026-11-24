# A node that rejoins its shard as a replica reports its primary's config
# epoch as it last knew it, which can be older than the primary's current one
# (e.g. a node restarted from an old nodes.conf). Receivers must not move any
# config epoch backwards because of it, least of all their own.

proc my_config_epoch {srv_idx} {
    return [CI $srv_idx cluster_my_epoch]
}

# Build a PING from 'sender_name' that says it is a replica of 'master_name',
# with the given epochs, and no gossip and no extensions.
proc build_replica_ping {sender_name sender_port master_name current_epoch config_epoch} {
    set CLUSTERMSG_TYPE_PING 0
    set CLUSTERMSG_MIN_LEN 2256
    set CLUSTER_NODE_SLAVE 2
    set packet [build_cluster_bus_header $sender_name $sender_port \
        [expr {$sender_port + 10000}] $CLUSTERMSG_TYPE_PING $CLUSTERMSG_MIN_LEN \
        0 $CLUSTER_NODE_SLAVE]
    # currentEpoch and configEpoch are at offsets 16 and 24, slaveof at 2128.
    set packet [string replace $packet 16 31 \
        [binary format WW $current_epoch $config_epoch]]
    set packet [string replace $packet 2128 2167 [binary format a40 $master_name]]
    return $packet
}

start_cluster 3 3 {tags {external:skip cluster} overrides {cluster-node-timeout 60000 cluster-allow-replica-migration no}} {
    test "Stale replica gossip does not move a primary's own config epoch backwards" {
        set node0_id [R 0 cluster myid]
        set node3_id [R 3 cluster myid]
        assert_equal $node0_id [dict get [cluster_get_myself 3] slaveof]

        # Node 3 takes over from node 0 while node 0 is stopped, so node 3
        # still sees node 0 as a primary (without slots).
        set node0_pid [srv 0 pid]
        pause_process $node0_pid
        R 3 cluster failover takeover
        wait_for_condition 1000 10 {
            [s -3 role] eq {master}
        } else {
            fail "node 3 did not take over"
        }
        set takeover_epoch [my_config_epoch 3]

        # Node 3's config epoch moves on: once another node holds the highest
        # epoch, BUMPEPOCH gives node 3 a new one.
        R 1 cluster bumpepoch
        set node1_epoch [my_config_epoch 1]
        wait_for_condition 1000 10 {
            [CI 3 cluster_current_epoch] >= $node1_epoch
        } else {
            fail "node 3 did not learn node 1's epoch"
        }
        R 3 cluster bumpepoch
        set current_epoch [my_config_epoch 3]
        assert {$current_epoch > $takeover_epoch}

        # Node 0 comes back as node 3's replica, still knowing node 3 by its
        # takeover epoch.
        set fd [send_cluster_bus_packet -3 [build_replica_ping $node0_id \
            [srv 0 port] $node3_id $takeover_epoch $takeover_epoch]]
        wait_for_log_messages -3 {"*A failover occurred in shard*"} 0 100 50
        close $fd

        assert_equal $current_epoch [my_config_epoch 3]
        assert_equal {master} [s -3 role]

        resume_process $node0_pid
        wait_for_condition 1000 50 {
            [s 0 role] eq {slave} && [s 0 master_link_status] eq {up}
        } else {
            fail "node 0 did not rejoin as a replica"
        }
        wait_for_cluster_propagation
        assert_equal $current_epoch [my_config_epoch 3]
    }
}
