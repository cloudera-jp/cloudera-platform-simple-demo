##
## Author: Nasheb Ismaily
## Description: Generates a random RFC5424 format syslog message
##


from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import OutputStreamCallback
import random
from datetime import datetime


class PyOutputStreamCallback(OutputStreamCallback):
    def __init__(self):
        pass


    def process(self, outputStream):
        hostname = "host"
        domain_name = ".example.com"
        tag = ["kernel", "python", "application"]
        version = "1"
        nouns = "application"
        verbs = ("started", "stopped", "exited", "completed")
        adv = ("successfully", "unexpectedly", "cleanly", "gracefully")


        for i in range(1,10):
            application = "{0}{1}".format(nouns, random.choice(range(1, 11)))
            message_id = "ID{0}".format(random.choice(range(1, 50)))
            random_tag = random.choice(tag)
            structured_data = "[SDID iut=\"{0}\" eventSource=\"{1}\" eventId=\"{2}\"]".format(random.choice(range(1, 10)), random_tag, random.choice(range(1, 100)))
            time_output = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            random_host = random.choice(range(1, 11))
            fqdn = "{0}{1}{2}".format(hostname, random_host, domain_name)
            random_pid = random.choice(range(500, 9999))
            priority = random.choice(range(0, 191))
            num = random.randrange(0, 4)
            random_message = application + ' has ' + verbs[num] + ' ' + adv[num]


            syslog_output = ("<{0}>{1} {2} {3} {4} {5} {6} {7} {8}\n".format(priority, version, time_output, fqdn, application,random_pid, message_id,structured_data, random_message))
            outputStream.write(bytearray(syslog_output.encode('utf-8')))


flowFile = session.create()
if (flowFile != None):
    flowFile = session.write(flowFile, PyOutputStreamCallback())
    session.transfer(flowFile, REL_SUCCESS)