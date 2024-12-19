
include { request } from 'plugin/nf-boost'

workflow {
  req = request('http://example.com', method: 'GET')
  println req.getResponseCode()
  println req.getInputStream().getText()
}
