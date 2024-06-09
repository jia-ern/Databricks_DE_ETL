# Databricks notebook source
# MAGIC %md
# MAGIC python --version 
# MAGIC
# MAGIC pip install  
# MAGIC
# MAGIC virtualenv -p /usr/bin/python3.7 databrickscli 
# MAGIC
# MAGIC source databrickscli/bin/activate 
# MAGIC
# MAGIC pip install databricks-cli 
# MAGIC (Generate token in databricks)
# MAGIC
# MAGIC databricks configure —token
# MAGIC (paste Databricks_workspace_url)  
# MAGIC (paste token)
# MAGIC   
# MAGIC databricks secrets list-scopes  
# MAGIC
# MAGIC databricks secrets create-scope --scope {scope_name} --initial-manage-principal users   
# MAGIC
# MAGIC databricks secrets list-scopes
# MAGIC
# MAGIC databricks secrets put --scope {scope_name} --key accesskey
# MAGIC (put in secrets)
# MAGIC
# MAGIC (press 1) 
# MAGIC
# MAGIC (paste > Ctrl-s > ctrl-x) 
# MAGIC
