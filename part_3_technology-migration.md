How would you identify the infrastructure component that is causing such issues?

1. we might use airflow which let us know about the ETL stage failure 
2. grafana dashboard to monitor backlog of messages
3. need to check for cpu usage and memory usage.

How would you start evaluating possible solutions?
based on the problem 
1. if kafka is bottleneck : try increasing more partitions , create more topic based on region
2. spark streaming : we are using sparkonk8s it scales automatically.
3. check for authentication like health checks 
4. Test cases

Technology :
1. tech meeting compliance with Company
2. Scalable and distributed
3. easily configurable and orchestrate ( airflow )
4. in-expensive and robust

How would you monitor the progress of your initiative?
1. link execution to strategy
2. Update jira or tools
3. Microservice developement
4. discussion with peers and stakeholders

Which roles would you need next to you to execute such an upgrade?
1. Cloud access to spawn up the services (EKS and s3 and DWH)
2. github access 
3. data access ( comppany twitter developer account)

Which tools would you use to plan and execute the project?

Kubernetes (EKS )
Cloudformation 
GitCi for CICD
Kafka to stream 
Spark 
Monitoring tool (Grafana and Elastic stack)
Python 
