##
## Description: Generates random Apache HTTPD combined log format lines
##


from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import OutputStreamCallback
import random
from datetime import datetime, timedelta


class PyOutputStreamCallback(OutputStreamCallback):
    def __init__(self):
        pass

    def process(self, outputStream):
        methods = ("GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH")
        paths = (
            "/",
            "/index.html",
            "/api/v1/users",
            "/api/v1/orders",
            "/images/logo.png",
            "/static/css/main.css",
            "/static/js/app.js",
            "/health",
            "/login",
            "/logout",
            "/search",
            "/products",
        )
        http_versions = ("HTTP/1.0", "HTTP/1.1", "HTTP/2.0")
        status_codes = (200, 201, 204, 301, 302, 304, 400, 401, 403, 404, 500, 502, 503)
        referers = (
            "-",
            "http://www.example.com/",
            "http://www.example.com/start.html",
            "https://www.google.com/",
            "https://example.com/dashboard",
        )
        user_agents = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
            "Mozilla/4.08 [en] (Win98; I ;Nav)",
            "curl/8.4.0",
            "Apache-HttpClient/4.5.14 (Java/11.0.21)",
            "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
        )
        timezones = ("+0000", "+0900", "-0700", "-0500", "+0100")
        for i in range(30):
            octet = lambda: random.choice(range(1, 255))
            client_ip = "{0}.{1}.{2}.{3}".format(octet(), octet(), octet(), octet())
            ident = "-"
            authuser = "-"
            auth_roll = random.choice(range(1, 10))
            if auth_roll == 1:
                authuser = "admin"
            elif auth_roll == 2:
                authuser = "user{0}".format(random.choice(range(1, 100)))
            offset_sec = random.choice(range(-86400, 86400))
            log_time = datetime.utcnow() + timedelta(seconds=offset_sec)
            tz = random.choice(timezones)
            date_str = log_time.strftime("%d/%b/%Y:%H:%M:%S") + " " + tz
            method = random.choice(methods)
            path = random.choice(paths)
            if path == "/products":
                path = "/products/{0}".format(random.choice(range(1, 500)))
            version = random.choice(http_versions)
            request = '{0} {1} {2}'.format(method, path, version)
            status = random.choice(status_codes)
            if status in (204, 304):
                bytes_sent = 0
            elif status >= 400:
                bytes_sent = random.choice(range(0, 512))
            else:
                bytes_sent = random.choice(range(128, 65536))
            referer = random.choice(referers)
            user_agent = random.choice(user_agents)

            log_line = '{0} {1} {2} [{3}] "{4}" {5} {6} "{7}" "{8}"\n'.format(
                client_ip,
                ident,
                authuser,
                date_str,
                request,
                status,
                bytes_sent,
                referer,
                user_agent,
            )
            outputStream.write(bytearray(log_line.encode("utf-8")))


flowFile = session.get()
if flowFile is None:
    flowFile = session.create()
flowFile = session.write(flowFile, PyOutputStreamCallback())
session.transfer(flowFile, REL_SUCCESS)
