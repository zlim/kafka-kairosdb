curl -X POST -H "Content-Type: application/json" --data @query_metrics.json http://localhost:8080/api/v1/datapoints/query | jq '.'
