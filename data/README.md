# 获取es状态

curl -X GET "http://localhost:9200/_cat/nodes?v&h=name,ram.percent,ram.max,heap.current,heap.max,disk.used,disk.total"