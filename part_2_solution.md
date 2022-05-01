# Rollout your pipeline 

Dashboards : Elastic search is implemented to update the dashboard 
New hashtags : added new hashtags as part of conf file / configmap in k8s

Regular update frequency : if it batch update we can consider using airflow to orcahestrate from data-lake to dashboards


### question 
 #### What roles do you need? 
 
     role to upgrade the helm chart version 
#### additional technologies
    airflow to configure the frequency
    CICD to deploy the changes 
    Prometheus : for additional monitoring 
    Linked Data HUB for data Lineage

#### additional features 
    
#### execution plan 
    using project management tool like jira for sprint planning 
    dividing the features into chunks 
    ( kafka -> spark-streaming) + (DWH modelling) + (cicd) + (monitoring)
    lastly : Data Quality checks and data lineage


        