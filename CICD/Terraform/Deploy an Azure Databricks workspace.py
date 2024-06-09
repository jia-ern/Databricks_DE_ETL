# Databricks notebook source
# MAGIC %md
# MAGIC The following sample configuration uses the azurerm Terraform provider to deploy an Azure Databricks workspace. For more information
# MAGIC about the azurerm Terraform plugin for Databricks, see [azurerm_databricks_workspace](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/databricks_workspace).
# MAGIC
# MAGIC #### Introduction
# MAGIC Below are the files you will end up with:
# MAGIC > - `main.tf`
# MAGIC > - `variables.tf`
# MAGIC
# MAGIC The below diagram outlines the high-level components that will be deployed.
# MAGIC
# MAGIC Pre-requisite: Download Terraform Executable
# MAGIC 1. Download Terraform Executable
# MAGIC 2. Extract Terraform Executable
# MAGIC - Extract the terraform.exe file to a directory of your choosing. Example: C:\terraform .
# MAGIC 3. Update the Global PATH Variable
# MAGIC - Click on the Start menu and search for Edit the system environment variables.
# MAGIC - On the System Properties window, select Environment Variables.
# MAGIC - Select the PATH variable, then click Edit.
# MAGIC - Click the New button, then type in the path from Step 2 where the Terraform executable is located. In the screenshot below, this is
# MAGIC C:\terraform .
# MAGIC 4. Step 4. Test the Configuration
# MAGIC - Open a new shell or command line program (Bash, PowerShell, or Command Prompt in Windows). Run the following Terraform command
# MAGIC to verify the PATH configuration. The command should return the current version of the download Terraform executable.
# MAGIC > `terraform -version`
# MAGIC
# MAGIC Click [here](https://jeffbrown.tech/install-terraform-windows/) to view more details.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Steps to Deploy in Visual Studio Code
# MAGIC 1. Install HashiCorp Terraform extension in Visual Studio Code
# MAGIC 2. After installing the above extension successfully, change the working directory to the current folder.
# MAGIC > `cd C:\terraform-databricks`
# MAGIC 3. Log in to Azure.
# MAGIC > `az login`
# MAGIC 4. Initializes a working directory containing Terraform configuration files.
# MAGIC > `terraform init`
# MAGIC 5. Validates the syntax and arguments of the Terraform configuration files in a directory, including argument and attribute names and types for resources and modules.
# MAGIC > `terraform validate`
# MAGIC 6. Creates an execution plan, which lets you preview the changes that Terraform plans to make to your infrastructure.
# MAGIC > `terraform plan`
# MAGIC 7. Executes the actions proposed in a Terraform plan.
# MAGIC > `terraform apply`
# MAGIC
# MAGIC #### Verify the Deployed Resources in Azure Portal
# MAGIC As shown below, Azure Databricks Workspace “ctprod-databricks“ is created in Azure Resource Group “ctprod-databricks“. A cluster
# MAGIC “ctprod-cluster“ is created in Azure Databricks Workspace “ctprod-databricks“.
# MAGIC
