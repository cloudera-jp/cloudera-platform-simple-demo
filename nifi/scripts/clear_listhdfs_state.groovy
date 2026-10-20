/**
 * NiFi ExecuteScript (Groovy): clear CLUSTER state on a ListHDFS processor via NiFi REST API.
 * Schedule this a few seconds before ListHDFS so each run performs a full directory listing.
 *
 * Dynamic properties (Processor Configuration):
 *   NiFi API Base URL  e.g. https://localhost:8443/nifi-api
 *   ListHDFS Processor Id  UUID of the ListHDFS Access Logs processor
 *   Authorization Header   optional, e.g. Bearer <token> when Knox/auth is enabled
 */
import java.net.HttpURLConnection
import java.net.URL

def flowFile = session.get()
if (flowFile == null) {
    flowFile = session.create()
}

def baseUrl = context.getProperty('NiFi API Base URL').evaluateAttributeExpressions(flowFile).getValue()
def processorId = context.getProperty('ListHDFS Processor Id').evaluateAttributeExpressions(flowFile).getValue()
def authHeader = context.getProperty('Authorization Header')
    ? context.getProperty('Authorization Header').evaluateAttributeExpressions(flowFile).getValue()
    : null

def endpoint = baseUrl.replaceAll('/+$', '') + '/processors/' + processorId + '/state/clear'
def conn = (HttpURLConnection) new URL(endpoint).openConnection()
conn.setRequestMethod('DELETE')
conn.setRequestProperty('Content-Type', 'application/json')
if (authHeader != null && !authHeader.isEmpty()) {
    conn.setRequestProperty('Authorization', authHeader)
}
conn.connect()
def status = conn.getResponseCode()
conn.disconnect()

flowFile = session.putAttribute(flowFile, 'listhdfs.state.clear.status', String.valueOf(status))
session.transfer(flowFile, REL_SUCCESS)
