/**
 * DEPRECATED for CFM/NiFi: ExecuteScript Groovy cannot load org.apache.hadoop.* unless
 * Hadoop client JARs are added via Module Directory (environment-specific).
 *
 * Prefer the native cleanup chain documented in nifi/flows/build_httpd_access_log_flow.py:
 *   ListHDFS -> UpdateAttribute -> RouteOnAttribute -> DeleteHDFS
 *
 * Optional workaround if you keep this script: set ExecuteScript "Module Directory" to the
 * Hadoop client lib directory on NiFi nodes (CDP example: /usr/lib/hadoop/lib).
 */
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.text.SimpleDateFormat

def flowFile = session.get()
if (flowFile == null) {
    flowFile = session.create()
}

def hdfsLogDirectory = flowFile.getAttribute('hdfs.log.directory')
if (hdfsLogDirectory == null || hdfsLogDirectory.isEmpty()) {
    hdfsLogDirectory = '/data/logs/httpd'
}
def retentionHours = flowFile.getAttribute('log.retention.hours')
def retentionMs = 6L * 60L * 60L * 1000L
if (retentionHours != null && !retentionHours.isEmpty()) {
    retentionMs = Long.parseLong(retentionHours) * 60L * 60L * 1000L
}

def prefix = 'access.log_'
def formats = [
    new SimpleDateFormat("yyyy-MM-dd-HH-mm"),
    new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss"),
]
formats.each { fmt -> fmt.setTimeZone(TimeZone.getTimeZone('UTC')) }

def conf = new Configuration()
def fs = FileSystem.get(conf)
def dir = new Path(hdfsLogDirectory)
def now = System.currentTimeMillis()
def deleted = 0

if (fs.exists(dir)) {
    fs.listStatus(dir).each { status ->
        if (!status.isFile()) {
            return
        }
        def name = status.getPath().getName()
        if (!name.startsWith(prefix)) {
            return
        }
        def tsStr = name.substring(prefix.length())
        def fileTime = null
        formats.each { fmt ->
            if (fileTime == null) {
                try {
                    fileTime = fmt.parse(tsStr).getTime()
                } catch (Exception ignored) {
                }
            }
        }
        if (fileTime != null && now - fileTime >= retentionMs) {
            if (fs.delete(status.getPath(), false)) {
                deleted++
            }
        }
    }
}

flowFile = session.putAttribute(flowFile, 'cleanup.deleted.count', String.valueOf(deleted))
session.transfer(flowFile, REL_SUCCESS)
