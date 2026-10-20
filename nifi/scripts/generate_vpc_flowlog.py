##
## Description: Generates AWS VPC Flow Log records (format version 2, space-separated).
## Source/dest addresses are chosen from CMDB nic IP list (see cmdb.ip.list.path).
##
## Inferred raw format from Splunk-enriched sample (_time, src_ip, dest_ip, app=ssl/443):
##   version account-id interface-id srcaddr dstaddr srcport dstport protocol
##   packets bytes start end action log-status
##


from org.apache.nifi.processor.io import OutputStreamCallback
import random
import time

# Override on NiFi node via flow variable / UpdateAttribute (cmdb.ip.list.path)
DEFAULT_IP_LIST_PATH = "/etc/nifi/conf/cmdb_nic_ips.txt"

AWS_ACCOUNT_ID = "123456789012"
INTERFACE_IDS = (
    "eni-0a1b2c3d4e5f67890",
    "eni-01ab23cd45ef67890",
    "eni-0fedcba9876543210",
)
# External peers (not necessarily in CMDB) for outbound/inbound Internet flows
EXTERNAL_IPS = (
    "221.0.1.1",
    "221.0.2.2",
    "203.0.113.10",
    "198.51.100.20",
    "8.8.8.8",
    "1.1.1.1",
)
DST_PORTS = (443, 443, 443, 80, 53, 8080, 8443)
PROTOCOLS = (6, 6, 6, 6, 17, 6)  # TCP / UDP


def load_cmdb_ips(path):
    ips = []
    try:
        reader = open(path, "r")
    except Exception:
        return ips
    try:
        for line in reader:
            value = line.strip()
            if value and value.count(".") == 3:
                ips.append(value)
    finally:
        reader.close()
    return ips


class PyOutputStreamCallback(OutputStreamCallback):
    def __init__(self, cmdb_ips):
        self.cmdb_ips = cmdb_ips

    def process(self, outputStream):
        if not self.cmdb_ips:
            raise Exception(
                "CMDB IP list is empty. Deploy cmdb_nic_ips.txt and set cmdb.ip.list.path."
            )

        for _ in range(30):
            eni = random.choice(INTERFACE_IDS)
            cmdb_ip = random.choice(self.cmdb_ips)
            external_ip = random.choice(EXTERNAL_IPS)

            # Mix direction: internal->Internet or Internet->internal (CMDB IP on either side)
            if random.choice((True, False)):
                srcaddr, dstaddr = cmdb_ip, external_ip
            else:
                srcaddr, dstaddr = external_ip, cmdb_ip

            srcport = random.choice(range(1024, 65535))
            dstport = random.choice(DST_PORTS)
            protocol = 6 if dstport != 53 else random.choice(PROTOCOLS)
            packets = random.choice(range(1, 50))
            if dstport in (443, 8443):
                byte_count = random.choice(range(500, 500000))
            elif dstport == 53:
                byte_count = random.choice(range(64, 512))
            else:
                byte_count = random.choice(range(100, 50000))

            offset = random.choice(range(-300, 0))
            end_ts = int(time.time()) + offset
            start_ts = end_ts - random.choice(range(60, 300))
            action = random.choice(("ACCEPT", "ACCEPT", "ACCEPT", "REJECT"))
            log_status = "OK"

            line = (
                "2 {account} {eni} {src} {dst} {srcport} {dstport} {proto} "
                "{packets} {bytes} {start} {end} {action} {status}\n"
            ).format(
                account=AWS_ACCOUNT_ID,
                eni=eni,
                src=srcaddr,
                dst=dstaddr,
                srcport=srcport,
                dstport=dstport,
                proto=protocol,
                packets=packets,
                bytes=byte_count,
                start=start_ts,
                end=end_ts,
                action=action,
                status=log_status,
            )
            outputStream.write(bytearray(line.encode("utf-8")))


flowFile = session.get()
if flowFile is None:
    flowFile = session.create()

ip_list_path = flowFile.getAttribute("cmdb.ip.list.path")
if ip_list_path is None or ip_list_path == "":
    ip_list_path = DEFAULT_IP_LIST_PATH

cmdb_ips = load_cmdb_ips(ip_list_path)
flowFile = session.write(flowFile, PyOutputStreamCallback(cmdb_ips))
session.transfer(flowFile, REL_SUCCESS)
