-- wrk -t8 -c256 -d30s -s bench/post.lua http://127.0.0.1:5555/ingest
wrk.method = "POST"
wrk.headers["Content-Type"] = "application/json"
counter = 0
request = function()
  counter = counter + 1
  wrk.body = string.format('{"device_id":%d,"temp":%.2f,"ok":true,"tag":"load-test"}', counter, 20 + (counter % 100) / 10)
  return wrk.format()
end