# Databricks notebook source
# MAGIC %run ../../../Includes/_common

# COMMAND ----------

@DBAcademyHelper.add_method
def create_DA_keys(self): 
    '''
    Create the DA references to the dev, prod and stage catalogs for the user.
    '''
    print('Set DA dynamic references to the dev, stage and prod catalogs.\n')
    setattr(DA, f'catalog_dev', f'{self.catalog_name}_1_dev')
    setattr(DA, f'catalog_stage', f'{self.catalog_name}_2_stage')
    setattr(DA, f'catalog_prod', f'{self.catalog_name}_3_prod')

# COMMAND ----------

@DBAcademyHelper.add_method
def create_volumes(self, in_catalog: str, in_schema: str, vol_names: list):
    """
    Creates the listed volumes in the specified schema within the user's catalog.
    
    This function will check if the volumes already exists. If they don't exist the volumes will be created and a note returned.
    
    Args:
    -------
        - in_catalog (str): The catalog to create the volume in.
        - in_schema (str): The schema to create the volumes in.
        - vol_names (list): A list of strings representing the volumes to create. If one volume, create a list of a single volume.
    
    Returns:
    -------
        Note in the log on the action(s) performed.
  
    Example:
    -------
        create_volumes(in_catalog=DA.catalog_dev_1, in_schema='default', vol_names=['health'])
    """
    ## Store current volumes in a list
    get_current_volumes = spark.sql(f'SHOW VOLUMES IN {in_catalog}.{in_schema}')
    current_volumes = (get_current_volumes
                       .select('volume_name')
                       .toPandas()['volume_name']
                       .tolist())

    ## Check to see if the volume(s) are created. If not, test each volume and create.
    if set(vol_names).issubset(current_volumes):
        print(f'Volume check. Volume {vol_names} already exist in {in_catalog}.{in_schema}. No action taken')

    for vol in vol_names:
        if vol not in current_volumes:
            spark.sql(f'CREATE VOLUME IF NOT EXISTS {in_catalog}.{in_schema}.{vol}')
            print(f'Created the volume: {in_catalog}.{in_schema}.{vol}.')

# COMMAND ----------

@DBAcademyHelper.add_method
def create_spark_data_frame_from_cdc(self, cdc_csv_file_path):
    '''
    Create the DataFrame used to create the CSV files for the course.
    '''
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import rand, when, lit, to_date, monotonically_increasing_id, col, coalesce
    from pyspark.sql.types import StringType
    import uuid

    ##
    ## Generate a column with a unique id (using it as a 'fake' PII column)
    ##

    # Define a UDF to generate deterministic UUID based on the row index
    def generate_deterministic_uuid(index):
        # Use UUID5 or UUID3 based on a namespace and the row index as the name
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, str(index)))  # You can use uuid3 as well

    # Register UDF
    generate_deterministic_uuid_udf = udf(generate_deterministic_uuid, StringType())

    ##
    ## Create spark dataframe with required columns and values to save CSV files.
    ##
    sdf = (spark
        .read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(cdc_csv_file_path)
        .repartition(1)
        .withColumn("ID", monotonically_increasing_id())
        .select("ID",
                generate_deterministic_uuid_udf(monotonically_increasing_id()).alias('PII'),
                when(col("ID") < 15121, "2025-01-01")
                    .when(col("ID") < 40351, "2025-01-02")
                    .otherwise("2025-01-03")
                    .alias("date"),
                when(col("HighChol") < .2, 0)  # when value is less than .2
                    .when((col("HighChol").cast('float') >= .2) & (col("HighChol") < 1.03), 1) # when value is between .2 and 1.03 
                    .otherwise(2)              # else when value is greater than or equal to 0.9
                    .alias("HighCholest"),
                "HighBP", 
                "BMI",
                "Age",
                "Education",
                "income"        
        )
    )

    return sdf

# COMMAND ----------

@DBAcademyHelper.add_method
def delete_source_files(self, source_files):
        """
        Deletes all files in the specified source volume.

        This function iterates through all the files in the given volume,
        deletes them, and prints the name of each file being deleted.

        Parameters:
        ----------
        source_files : str, optional
            The path to the volume containing the files to delete. 
            Use the {DA.paths.working_dir} to dynamically navigate to the user's volume location in dbacademy/ops/vocareumlab@name:
                Example: DA.paths.working_dir = /Volumes/dbacademy/ops/vocareumlab@name

        Returns:
        -------
        None
            This function does not return any value. It performs file deletion as a side effect and prints all files that it deletes.

        Example:
        --------
        delete_source_files(f'{DA.paths.working_dir}/pii/stream_source/user_reg')
        """
        
        import os
        
        print(f'\nSearching for files in {source_files} volume to delete prior to creating files...')
        if os.path.exists(source_files):
            list_of_files = sorted(os.listdir(source_files))
        else:
            list_of_files = None

        if not list_of_files:  # Checks if the list is empty.
            print(f"No files found in {source_files}.\n")
        else:
            for file in list_of_files:
                file_to_delete = source_files + '/' + file
                print(f'Deleting file: {file_to_delete}')
                dbutils.fs.rm(file_to_delete)

# COMMAND ----------

##
## CREATING DLT PIPELINES USING THE DATABRICKS SDK
## 

import os
from databricks.sdk.service import pipelines
from databricks.sdk import WorkspaceClient

class DAPipelineConfig:
    """
    A custom class for Databricks Academy courses used to dynamically create and manage Delta Live Tables (DLT) pipelines 
    in Databricks using the Databricks SDK.

    This class simplifies the creation and management of DLT pipelines by setting up 
    the necessary configurations, checking for duplicates, and providing methods 
    to create, start, and delete pipelines.

    Attributes:
        pipeline_name (str): The name of the DLT pipeline.
        catalog (str): The catalog in which the pipeline will be created.
        schema (str): The schema for the pipeline.
        pipeline_notebooks (list): List of notebooks to run in the pipeline. 
                                   Notebooks should be specified by their relative paths 
                                   starting from the directory where this notebook is executed.
        serverless (bool): Flag indicating whether the pipeline will be serverless. Defaults to True.
        continuous (bool): Flag indicating whether the pipeline will be continuous. Defaults to False.
        config_variables (dict): A dictionary of configuration variables to pass to the pipeline. Defaults to None.
        development (bool): Flag indicating whether the pipeline is in development mode. Defaults to True.
        current_root_path (str): The root directory path where the notebooks are located.
        full_notebook_paths (list): List of full paths to the notebooks specified for the pipeline.
        w (WorkspaceClient): A client instance for interacting with the Databricks workspace.
        current_pipeline_id (str): The ID of the created pipeline, assigned after the pipeline creation.

    Example Usage:
        pipeline = DAPipelineConfig(
            pipeline_name=f"sdk_{DA.catalog_dev}", 
            catalog=f"{DA.catalog_dev}", 
            schema="default", 
            # Specify the relative notebook paths starting from the current notebook's directory.
            pipeline_notebooks=[ 
                "src/dlt_pipelines/ingest-bronze-silver_dlt", 
                "src/dlt_pipelines/gold_tables_dlt"
            ],
            config_variables={
                'target': 'dev', 
                'raw_data_path': f'/Volumes/{DA.catalog_dev}/default/health'
            }
        )

        # Create and start the DLT pipeline
        pipeline.create_dlt_pipeline()
        pipeline.start_dlt_pipeline()
    """


    def __init__(self, pipeline_name: str, catalog: str, schema: str, pipeline_notebooks: list, serverless: bool = True, continuous: bool = False, config_variables: dict = None, development: bool = True):
        """
        Initializes the DAPipelineConfig instance.

        Args:
            pipeline_name (str): The name of the DLT pipeline.
            catalog (str): The catalog to use for the pipeline.
            schema (str): The schema for the pipeline.
            pipeline_notebooks (list): List of notebooks to run in the pipeline.
            serverless (bool, optional): If True, use serverless clusters. Defaults to True.
            continuous (bool, optional): If True, use continuous pipelines. Defaults to False.
            config_variables (dict, optional): Configuration variables for the pipeline. Defaults to None.
            development (bool, optional): If True, the pipeline is in development mode. Defaults to True.
        """
        self.pipeline_name = pipeline_name
        self.catalog = catalog
        self.schema = schema
        self.pipeline_notebooks = pipeline_notebooks
        self.current_root_path = self.get_current_directory_path()     ## Get the path of where the this class was created
        self.full_notebook_paths = self.set_notebook_paths()           ## add the executing notebook path to the notebooks specified in the class.
        self.serverless = serverless
        self.continuous = continuous
        self.config_variables = config_variables
        self.development = development

        ## Connect the SDK
        self.w = self.get_workspace_client()

        ## Check if the pipeline name is available
        self.duplicate_pipeline_name()

        print(' ---- CREATING A DLT PIPELINE USING THE DATABRICKS SDK ----')
        

    def get_workspace_client(self):
        """
        Establishes and returns a WorkspaceClient instance for interacting with the Databricks API.
        This is set when the object is created within self.w

        Returns:
            WorkspaceClient: A client instance to interact with the Databricks workspace.
        """
        w = WorkspaceClient()
        return w
    
    ## Check for duplicate name. Return error if name already found.
    def duplicate_pipeline_name(self):
        """
        Checks if a pipeline with the same name already exists. If it does, raises an error.

        Raises:
            AssertionError: If a pipeline with the same name already exists in the workspace.
        """
        all_dlt_pipelines = self.w.pipelines.list_pipelines()
        for pipeline in all_dlt_pipelines:
            # If a pipeline with the same name exists, raise an error
            if pipeline.name == self.pipeline_name:
                assert_false = False
                assert assert_false, f'You already have pipeline named {self.pipeline_name}. Please go to the Delta Live Tables page and manually delete the pipeline. Then rerun this program to create the pipeline.'


    def get_current_directory_path(self):
        """
        Gets the path of the notebook where this code is executed. This helps to locate the notebook files for the DLT pipeline. From the main course folder.

        Returns:
            str: The path to the current directory in Databricks.
        """
        current_path = os.getcwd()

        ## Go back two folders to the main course folder
        current_root_path = os.path.dirname(os.path.dirname(current_path))
        return current_root_path
    
        # ## Get path of includes folder
        # current_folder_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)

        # ## Go back one folder to the main course folder
        # current_root_path = "/Workspace" + "/".join(current_folder_path.split("/")[:-1]) + '/'
        # return current_root_path


    def set_notebook_paths(self):
        """
        Constructs full notebook paths for each notebook specified in the `pipeline_notebooks` attribute. The notebooks in that location specify the path from where this code is executed to enable the class to be a bit more dynamic when specifying paths.

        Returns:
            list: A list of full notebook paths using the path where this notebook was executed.
        
        Raises:
            AssertionError: If any of the notebook paths do not exist.
        """
        ## Create paths for each notebook specified in method argument notebooks(list of notebooks to use)

        # Append base_path to each folder
        full_notebook_paths = [f"{self.current_root_path}{notebook}" for notebook in  self.pipeline_notebooks]
        
        for notebook in full_notebook_paths:
            # Attempt to list the contents of the path. If the path does not exist return an error.
            if os.path.exists(notebook):
                pass
            else:
                assert_false = False
                assert assert_false, f'The notebook path you specified does not exists {notebook}. \nPlease specify a correct path in the  pipeline_notebooks. Provide a path based on the current directory this is being run in, not the entire path.'
            
        return full_notebook_paths
    

    def create_dlt_pipeline(self):
        """
        Creates a Delta Live Tables (DLT) pipeline using the provided configuration.

        This method uses the Databricks SDK to create the pipeline and assigns the pipeline ID to the class instance.

        Raises:
            Exception: If the pipeline creation fails.
        """
        
        # Create the DLT pipeline using the Databricks SDK
        pipeline_info = self.w.pipelines.create(
            allow_duplicate_names = False,
            name = self.pipeline_name,
            catalog = self.catalog,
            target = self.schema,
            serverless = self.serverless,
            continuous = self.continuous,
            configuration = self.config_variables,
            development = self.development,
            libraries = [pipelines.PipelineLibrary(notebook=pipelines.NotebookLibrary(path=notebook)) for notebook in self.full_notebook_paths]
        )
        # Save the pipeline ID to the class instance
        self.current_pipeline_id = pipeline_info.pipeline_id
        print(f'Creating DLT pipeline: {self.pipeline_name}. \nDLT pipeline id: {self.current_pipeline_id}.\nNavigate to the Delta Live Tables UI to view the pipeline.')
    


    def start_dlt_pipeline(self):
        """
        Starts the DLT pipeline that was previously created.

        If no pipeline ID exists, it prompts the user to first create the pipeline.

        Raises:
            AssertionError: If no pipeline ID exists to start.
        """
        if hasattr(self, 'current_pipeline_id') and self.current_pipeline_id is not None:
            self.w.pipelines.start_update(self.current_pipeline_id)
            print('----------------------------------------------')
            print(f'Starting pipeline {self.pipeline_name} with id {self.current_pipeline_id}')
            print('----------------------------------------------')
        else:
            print("No pipeline id exists for this object. Please create the pipeline first using the class.")


    def delete_dlt_pipeline(self):
        """
        Deletes the DLT pipeline that was previously created.

        If no pipeline ID exists, it prompts the user to first create the pipeline.

        Raises:
            AssertionError: If no pipeline ID exists to delete.
        """
        if hasattr(self, 'current_pipeline_id') and self.current_pipeline_id is not None:
            self.w.pipelines.delete(pipeline_id = self.current_pipeline_id)
            print('----------------------------------------------')
            print(f'Deleting pipeline {self.pipeline_name} with id {self.current_pipeline_id}')
            print('----------------------------------------------')
        else:
            print("No pipeline id exists to delete for this object. Please create the pipeline first using the class.")

# COMMAND ----------

DA = DBAcademyHelper()
DA.init()