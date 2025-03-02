# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 3.1 - Deploying the Databricks Assets
# MAGIC
# MAGIC In Databricks, you have several options for deploying your Databricks Assets such as the UI, REST APIs, Databricks CLI, Databricks SDK or Databricks Asset Bundles (DABs). Databricks recommends Databricks Asset Bundles for creating, developing, deploying, and testing jobs and other Databricks resources as source code. 
# MAGIC
# MAGIC In this demonstration, we will deploy our project and explore the job and pipeline using the Workflows UI. Then, we will examine the Workflow JSON and YAML structures and discuss how we can use these within our CI/CD process.
# MAGIC
# MAGIC ## Objectives
# MAGIC - Deploy Databricks assets using the Databricks SDK. 
# MAGIC - Analyze the Workflow JSON and YAML definitions for jobs and tasks, and explore their role in enabling automated deployment.

# COMMAND ----------

# MAGIC %md
# MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
# MAGIC
# MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default.
# MAGIC
# MAGIC Follow these steps to select the classic compute cluster:
# MAGIC
# MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
# MAGIC
# MAGIC 1. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
# MAGIC
# MAGIC   - In the drop-down, select **More**.
# MAGIC
# MAGIC   - In the **Attach to an existing compute resource** pop-up, select the first drop-down. You will see a unique cluster name in that drop-down. Please select that cluster.
# MAGIC
# MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
# MAGIC
# MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
# MAGIC
# MAGIC 1. Find the triangle icon to the right of your compute cluster name and click it.
# MAGIC
# MAGIC 1. Wait a few minutes for the cluster to start.
# MAGIC
# MAGIC 1. Once the cluster is running, complete the steps above to select your cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC Run the following cell to configure your working environment for this course. 
# MAGIC
# MAGIC **NOTE:** The `DA` object is only used in Databricks Academy courses and is not available outside of these courses. It will dynamically reference the information needed to run the course.
# MAGIC
# MAGIC ##### The notebook "2.1 - Modularizing PySpark Code - Required" sets up the catalogs for this course. If you have not run this notebook, the catalogs will not be available.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-3.1

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Create the Databricks Job with Notebooks, Python Files and DLT
# MAGIC
# MAGIC In this section, we will create the job for our project. The job will contain the following tasks:
# MAGIC - Unit tests
# MAGIC - DLT pipeline with the ETL pipeline and integration tests 
# MAGIC - Final data visualization deliverable for this project
# MAGIC
# MAGIC **FINAL JOB**
# MAGIC
# MAGIC ![Final SDK Workflow](../Includes/images/05_final_sdk_workflow.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. During development, it's beneficial to build out your Workflow and/or DLT Pipeline using the UI. The UI provides an easy way to build your desired job, and it generates the necessary JSON or YAML files to start automating your deployment across different environments.
# MAGIC
# MAGIC     **NOTE:** In the cell below, we will create the job using custom functions provided by Databricks Academy, which leverages the Databricks SDK behind the scenes. **This approach saves time in class by avoiding the need to manually create the Workflow using the UI for the demonstration. Workflows are a prerequisite for this course.**

# COMMAND ----------

## Confirm the pipeline from the previous demonstration exists. If not, create the DLT pipeline and store the ID
my_pipeline_id = obtain_pipeline_id_or_create_if_not_exists()
print(my_pipeline_id)

# ## Create the job
create_demo_5_job(my_pipeline_id = my_pipeline_id, job_name = f'Dev Workflow Using the SDK_{DA.catalog_name}')

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Complete the following steps to view and run the new job:
# MAGIC
# MAGIC     a. In the far-left navigation bar, right-click on **Workflows** and select *Open Link in New Tab*.
# MAGIC
# MAGIC     b. In the **Jobs** tab, you should see a job named **Dev Workflow Using the SDK_user_name**.
# MAGIC
# MAGIC     c. Select the job **Dev Workflow Using the SDK_user_name**.
# MAGIC
# MAGIC     d. Select on **Run now** to run the job.
# MAGIC
# MAGIC     e. Leave the job open.

# COMMAND ----------

# MAGIC %md
# MAGIC 4. While the job is running, explore the job tasks. The job will take between 5-7 minutes to complete. Navigate to the job **Runs** tab. Here you should see the job executing.
# MAGIC
# MAGIC <br></br>
# MAGIC
# MAGIC **Job Task Descriptions**
# MAGIC ![Final SDK Workflow Desc](../Includes/images/05_Final_Workflow_Desc.png)
# MAGIC
# MAGIC #####4a. Task 1: Unit_Tests
# MAGIC
# MAGIC    a. On the job **Runs** tab, right click on the square for **Unit_Tests** and select *Open Link in New Tab*.
# MAGIC
# MAGIC    b. Notice the **Run Unit Tasks** notebook executes the unit tests we created earlier.
# MAGIC
# MAGIC    c. Close the tab.
# MAGIC
# MAGIC <br></br>
# MAGIC
# MAGIC #####4b. Task 2: Health_ETL
# MAGIC
# MAGIC    a. Select the **Tasks** tab and select the **Health_ETL** task*.
# MAGIC
# MAGIC    b. In the **Task** section find the **Pipeline** value and select the icon to the right of the pipeline name to open the pipeline.
# MAGIC
# MAGIC   **NOTE:** If the DLT pipeline has already completed, simply select the pipeline link.
# MAGIC
# MAGIC    c. Notice the pipeline executes (will execute) the ETL pipeline we created earlier.
# MAGIC
# MAGIC    d. Close the DLT pipeline.
# MAGIC
# MAGIC <br></br>
# MAGIC
# MAGIC #####4c. Task 3: Visualization
# MAGIC    a. Select the job **Runs** tab, right click on the square for **Visualization** and select *Open Link in New Tab*.
# MAGIC
# MAGIC    b. Notice the **Final Visualization** notebook creates the final visualization for the project.
# MAGIC
# MAGIC    c. Close the tab.
# MAGIC
# MAGIC
# MAGIC Leave the job open while it continues to run and continue the next steps.

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Complete the following to view the JSON file to deploy this job:
# MAGIC
# MAGIC    a. Navigate back to the main Workflow job by selecting the **Tasks** tab.
# MAGIC
# MAGIC    b. At the top right of the job, select the kebab menu (three ellipsis icon near the **Run now** button).
# MAGIC
# MAGIC    c. Select **View JSON**.
# MAGIC
# MAGIC    d. Notice that you can view the JSON definition for the job for use with the REST API. This is a great way to easily obtain the necessary values to begin automating your deployment for the REST API (the SDK values are similar).
# MAGIC
# MAGIC    e. Close the **Job JSON** popup.

# COMMAND ----------

# MAGIC %md
# MAGIC 6. Complete the following to view the YAML file to deploy this job:
# MAGIC
# MAGIC    a. Confirm you are on the **Tasks** tab.
# MAGIC
# MAGIC    b. At the top right of the job, select the kebab menu (three ellipsis icon near the **Run now** button).
# MAGIC
# MAGIC    c. Select **Switch to code version (YAML)**.
# MAGIC
# MAGIC    d. Notice that you can view job as the YAML for the job for. This is a great way to easily obtain the necessary values for the YAML deployment (this YAML file is extremely help when deploying using **Databricks Asset Bundles**).
# MAGIC
# MAGIC    e. In the top right select **Switch to visual mode**.

# COMMAND ----------

# MAGIC %md
# MAGIC 7. The job should be completed by now. View the completed job and confirm the three tasks completed successfully. Feel free to view the completed tasks.

# COMMAND ----------

# MAGIC %md
# MAGIC 8. Complete the following steps to view the Databricks SDK code to create the Workflow.
# MAGIC
# MAGIC     a. SDK Code: **[../Includes/Classroom-Setup-3.1]($../Includes/Classroom-Setup-3.1)**
# MAGIC
# MAGIC     b. Scroll down to cell 4: `def create_demo_5_job(my_pipeline_id, job_name)` 
# MAGIC     
# MAGIC     c. Notice that amount of Python code used to create the Job to deploy our development code. 
# MAGIC     
# MAGIC     **NOTE:** Details of the Databricks SDK code is beyond the scope of this course.
# MAGIC
# MAGIC While the SDK provides low-level control over your deployment, it also requires significant time and effort to write all the necessary code.  
# MAGIC
# MAGIC In this example, we are only deploying the development job. Additional modifications will be needed to deploy both the staging and production jobs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps for CI/CD
# MAGIC
# MAGIC Think about the following for the deploying this project utilizing the entire CI/CD process:
# MAGIC - How will I automatically deploy the databricks assets to run for **dev**, **stage**, and **prod** environments?
# MAGIC - How do I parameterize all the values I need based on the target environment?
# MAGIC - How do I configure the necessary variables for the DLT pipeline during each deployment?
# MAGIC - How do I maintain all of the code?
# MAGIC - How do I automate this entire process?
# MAGIC - How can I setup continuous integration and continuous delivery or deployment (CI/CD) system, such as GitHub Actions, to automatically run your unit tests whenever your code changes? For an example, see the coverage of GitHub Actions in [Software engineering best practices for notebooks](https://docs.databricks.com/en/notebooks/best-practices.html). 
# MAGIC
# MAGIC Next Steps: Learn more about [**Databricks Asset Bundles (DABs)**](https://docs.databricks.com/en/dev-tools/bundles/index.html) for deploying your Databricks Assets!

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>