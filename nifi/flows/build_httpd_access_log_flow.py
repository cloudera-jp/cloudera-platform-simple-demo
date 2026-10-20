#!/usr/bin/env python3
"""Build NiFi 2.3 Flow Definition JSON for HTTPD access log generation."""

import json
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCRIPT_DIR = ROOT / "scripts"
OUT = Path(__file__).resolve().parent / "httpd-access-log-generator.flow.json"

NIFI_VERSION = "2.3.0"


def uid():
    return str(uuid.uuid4())


def iid():
    return str(uuid.uuid4())


def proc(
    identifier,
    instance_id,
    group_id,
    name,
    class_name,
    artifact,
    x,
    y,
    properties,
    schedule="0 */5 * * * ?",
    strategy="CRON_DRIVEN",
    auto_term=None,
    comments="",
    execution_node="ALL",
):
    if auto_term is None:
        auto_term = []
    return {
        "identifier": identifier,
        "instanceIdentifier": instance_id,
        "name": name,
        "comments": comments,
        "position": {"x": x, "y": y},
        "bundle": {
            "group": "org.apache.nifi",
            "artifact": artifact,
            "version": NIFI_VERSION,
        },
        "style": {},
        "type": class_name,
        "properties": properties,
        "propertyDescriptors": {},
        "schedulingPeriod": schedule,
        "schedulingStrategy": strategy,
        "executionNode": execution_node,
        "penaltyDuration": "30 sec",
        "yieldDuration": "1 sec",
        "bulletinLevel": "WARN",
        "runDurationMillis": 0,
        "concurrentlySchedulableTaskCount": 1,
        "autoTerminatedRelationships": auto_term,
        "scheduledState": "ENABLED",
        "componentType": "PROCESSOR",
        "groupIdentifier": group_id,
    }


def conn(identifier, instance_id, group_id, src, dst, rels, bends=None):
    if bends is None:
        bends = []
    return {
        "identifier": identifier,
        "instanceIdentifier": instance_id,
        "name": "",
        "source": src,
        "destination": dst,
        "labelIndex": 1,
        "zIndex": 0,
        "selectedRelationships": rels,
        "backPressureObjectThreshold": 10000,
        "backPressureDataSizeThreshold": "1 GB",
        "flowFileExpiration": "0 sec",
        "prioritizers": [],
        "bends": bends,
        "loadBalanceStrategy": "DO_NOT_LOAD_BALANCE",
        "partitioningAttribute": "",
        "loadBalanceCompression": "DO_NOT_COMPRESS",
        "componentType": "CONNECTION",
        "groupIdentifier": group_id,
    }


def endpoint(proc_id, instance_id, group_id, name):
    return {
        "id": proc_id,
        "type": "PROCESSOR",
        "groupId": group_id,
        "name": name,
        "comments": "",
        "instanceIdentifier": instance_id,
    }


def main():
    generate_py = (SCRIPT_DIR / "generate_httpd.py").read_text(encoding="utf-8")

    group_id = uid()
    group_iid = iid()

    p_trigger = uid()
    p_trigger_i = iid()
    p_gen = uid()
    p_gen_i = iid()
    p_attr = uid()
    p_attr_i = iid()
    p_put = uid()
    p_put_i = iid()
    p_list = uid()
    p_list_i = iid()
    p_attr_del = uid()
    p_attr_del_i = iid()
    p_route = uid()
    p_route_i = iid()
    p_delete = uid()
    p_delete_i = iid()

    hadoop_conf = "/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml"
    hdfs_dir = "/data/logs/httpd"
    # Filename embeds creation time (see Set HDFS Filename). ListHDFS state must be cleared each run.
    log_epoch_attr = "${filename:substring(11):toDate('yyyy-MM-dd-HH-mm'):toNumber()}"
    retention_route = "${now():toNumber():minus(21600000):gt(${log.file.epoch:toNumber()})}"

    processors = [
        proc(
            p_trigger,
            p_trigger_i,
            group_id,
            "Trigger Every 5 Minutes",
            "org.apache.nifi.processors.standard.GenerateFlowFile",
            "nifi-standard-nar",
            0,
            0,
            {
                "File Size": "0B",
                "Batch Size": "1",
                "Data Format": "Text",
                "Unique FlowFiles": "false",
                "character-set": "UTF-8",
            },
            comments="Cron: every 5 minutes. Triggers log generation only.",
        ),
        proc(
            p_gen,
            p_gen_i,
            group_id,
            "Generate HTTPD Access Logs",
            "org.apache.nifi.processors.script.ExecuteScript",
            "nifi-scripting-nar",
            400,
            -120,
            {
                "Script Engine": "python",
                "Script Body": generate_py,
            },
            schedule="0 sec",
            strategy="TIMER_DRIVEN",
            comments="Runs generate_httpd.py (30 lines per FlowFile). Requires execute code permission.",
        ),
        proc(
            p_attr,
            p_attr_i,
            group_id,
            "Set HDFS Filename",
            "org.apache.nifi.processors.attributes.UpdateAttribute",
            "nifi-update-attribute-nar",
            800,
            -120,
            {
                "Store State": "Do not store state",
                "canonical-value-lookup-cache-size": "100",
                "filename": "access.log_${now():format('yyyy-MM-dd')}T${now():format('HH:mm:ss')}",
                "mime.type": "text/plain",
            },
            schedule="0 sec",
            strategy="TIMER_DRIVEN",
        ),
        proc(
            p_put,
            p_put_i,
            group_id,
            "PutHDFS Access Log",
            "org.apache.nifi.processors.hadoop.PutHDFS",
            "nifi-hadoop-nar",
            1200,
            -120,
            {
                "Hadoop Configuration Resources": hadoop_conf,
                "Directory": hdfs_dir,
                "Conflict Resolution Strategy": "replace",
                "writing-strategy": "Write and rename",
                "Compression codec": "NONE",
            },
            schedule="0 sec",
            strategy="TIMER_DRIVEN",
            auto_term=["success"],
            comments="Configure Kerberos on this processor if the cluster uses secure HDFS.",
        ),
        proc(
            p_list,
            p_list_i,
            group_id,
            "ListHDFS Access Logs",
            "org.apache.nifi.processors.hadoop.ListHDFS",
            "nifi-hadoop-nar",
            400,
            120,
            {
                "Hadoop Configuration Resources": hadoop_conf,
                "Directory": hdfs_dir,
                "Recurse Subdirectories": "false",
                "File Filter": "access\\.log_.*",
                "file-filter-mode": "Files Only",
            },
            schedule="30 0/5 * * * ?",
            strategy="CRON_DRIVEN",
            comments=(
                "Lists candidate files for retention cleanup. CLUSTER state = incremental listing only; "
                "clear processor state before each run (or use clear_listhdfs_state.groovy at :00). "
                "hdfs.lastModified is HDFS mtime at list time, not updated by Route."
            ),
            execution_node="PRIMARY",
        ),
        proc(
            p_attr_del,
            p_attr_del_i,
            group_id,
            "Set HDFS Delete Path",
            "org.apache.nifi.processors.attributes.UpdateAttribute",
            "nifi-update-attribute-nar",
            800,
            120,
            {
                "Store State": "Do not store state",
                "canonical-value-lookup-cache-size": "100",
                "hdfs.delete.path": "${path}/${filename}",
                "log.file.epoch": log_epoch_attr,
            },
            schedule="0 sec",
            strategy="TIMER_DRIVEN",
        ),
        proc(
            p_route,
            p_route_i,
            group_id,
            "Route Files Older Than 6 Hours",
            "org.apache.nifi.processors.standard.RouteOnAttribute",
            "nifi-standard-nar",
            1200,
            120,
            {
                "Routing Strategy": "Route to Property name",
                "delete": retention_route,
            },
            schedule="0 sec",
            strategy="TIMER_DRIVEN",
            auto_term=["unmatched"],
        ),
        proc(
            p_delete,
            p_delete_i,
            group_id,
            "DeleteHDFS Access Log",
            "org.apache.nifi.processors.hadoop.DeleteHDFS",
            "nifi-hadoop-nar",
            1600,
            120,
            {
                "Hadoop Configuration Resources": hadoop_conf,
                "file_or_directory": "${hdfs.delete.path}",
                "recursive": "false",
            },
            schedule="0 sec",
            strategy="TIMER_DRIVEN",
            auto_term=["success"],
            comments="Configure Kerberos same as PutHDFS.",
        ),
    ]

    connections = [
        conn(
            uid(),
            iid(),
            group_id,
            endpoint(p_trigger, p_trigger_i, group_id, "Trigger Every 5 Minutes"),
            endpoint(p_gen, p_gen_i, group_id, "Generate HTTPD Access Logs"),
            ["success"],
        ),
        conn(
            uid(),
            iid(),
            group_id,
            endpoint(p_gen, p_gen_i, group_id, "Generate HTTPD Access Logs"),
            endpoint(p_attr, p_attr_i, group_id, "Set HDFS Filename"),
            ["success"],
        ),
        conn(
            uid(),
            iid(),
            group_id,
            endpoint(p_attr, p_attr_i, group_id, "Set HDFS Filename"),
            endpoint(p_put, p_put_i, group_id, "PutHDFS Access Log"),
            ["success"],
        ),
        conn(
            uid(),
            iid(),
            group_id,
            endpoint(p_list, p_list_i, group_id, "ListHDFS Access Logs"),
            endpoint(p_attr_del, p_attr_del_i, group_id, "Set HDFS Delete Path"),
            ["success"],
        ),
        conn(
            uid(),
            iid(),
            group_id,
            endpoint(p_attr_del, p_attr_del_i, group_id, "Set HDFS Delete Path"),
            endpoint(p_route, p_route_i, group_id, "Route Files Older Than 6 Hours"),
            ["success"],
        ),
        conn(
            uid(),
            iid(),
            group_id,
            endpoint(p_route, p_route_i, group_id, "Route Files Older Than 6 Hours"),
            endpoint(p_delete, p_delete_i, group_id, "DeleteHDFS Access Log"),
            ["delete"],
        ),
    ]

    flow = {
        "flowEncodingVersion": "1.0",
        "parameterContexts": {},
        "parameterProviders": {},
        "parameterContextName": "",
        "externalControllerServices": {},
        "flowContents": {
            "identifier": group_id,
            "instanceIdentifier": group_iid,
            "name": "HTTPD Access Log Generator",
            "comments": (
                "Generates Apache HTTPD combined logs every 5 minutes, writes to HDFS under "
                "/data/logs/httpd, and deletes files older than 6 hours (filename timestamp; ListHDFS full scan)."
            ),
            "position": {"x": 0, "y": 0},
            "processGroups": [],
            "remoteProcessGroups": [],
            "processors": processors,
            "inputPorts": [],
            "outputPorts": [],
            "connections": connections,
            "labels": [],
            "funnels": [],
            "controllerServices": [],
            "variables": {
                "hdfs.config.resources": hadoop_conf,
                "hdfs.log.directory": hdfs_dir,
                "log.retention.hours": "6",
            },
            "defaultFlowFileExpiration": "0 sec",
            "defaultBackPressureObjectThreshold": 10000,
            "defaultBackPressureDataSizeThreshold": "1 GB",
            "flowFileConcurrency": "UNBOUNDED",
            "flowFileOutboundPolicy": "STREAM_WHEN_AVAILABLE",
            "componentType": "PROCESS_GROUP",
        },
    }

    OUT.write_text(json.dumps(flow, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
