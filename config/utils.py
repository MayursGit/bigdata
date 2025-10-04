import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import md5, concat_ws, col, lit, current_timestamp, current_date
from delta.tables import DeltaTable
import logging
import sys
import os
from typing import Optional, Union
import importlib
import pkgutil
sys.path.append("/Workspace/Users/mayur10594@gmail.com/ETL_project")
import schemas

def load_config(path: str, env: str) -> dict:
    """
    Load configuration from a YAML file and extract settings for a specific environment.
    (Assumes environments like 'dev', 'test', 'prod' are top-level keys in YAML)

    Args:
        path (str): Path to the YAML configuration file.
        env (str): Environment name to load (e.g., 'dev', 'prod').

    Returns:
        dict: Configuration dictionary for the specified environment.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path, "r") as f:
        try:
            config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Failed to parse YAML file '{path}': {e}")

    if not isinstance(config, dict):
        raise ValueError(f"Invalid YAML format: expected a dictionary at root, got {type(config).__name__}")

    if env not in config:
        raise KeyError(f"Environment '{env}' not found in YAML config")

    return config[env]


def load_sqls(path,file):
    file_path=f"{path}/{file}"
    with open(file_path, "r") as f:
        return f.read()

def load_all_schemas():
    schema_map = {}
    # Get actual filesystem path
    schemas_path = os.path.dirname(schemas.__file__)
    print("Schemas folder path:", schemas_path)

    for file in os.listdir(schemas_path):
        if file.endswith(".py") and file != "__init__.py":
            module_name = file[:-3]  # strip .py
            print("Found schema module:", module_name)
            module = importlib.import_module(f"schemas.{module_name}")

            for attr_name in dir(module):
                if attr_name.endswith("_schema") or attr_name == "schema":
                    schema_obj = getattr(module, attr_name)
                    version = attr_name.replace("_schema", "")
                    key = f"{module_name.replace('_schema','')}.{version}" if version else module_name
                    schema_map[key] = schema_obj
                    print("Loaded schema:", key)
    return schema_map

# Usage
# schemas_map = load_all_schemas()
# print("All schemas loaded:", schemas_map.keys())

class DefaultContextFilter(logging.Filter):
    """Ensures log record always has context fields like job_id, run_id, task_key, env."""
    def __init__(self, context: dict):
        super().__init__()
        self.context = context or {}

    def filter(self, record):
        for k, v in self.context.items():
            if not hasattr(record, k):
                setattr(record, k, v)
        return True


def _get_job_context() -> dict:
    """
    Automatically fetch Databricks Job context (job_id, run_id, task_key, notebook).
    Returns defaults if running interactively.
    """
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        job_id = str(ctx.jobId().get()) if ctx.jobId().isDefined() else "manual"
        run_id = str(ctx.currentRunId().toString()) if ctx.currentRunId().isDefined() else "local"
        task_key = str(ctx.taskKey().get()) if ctx.taskKey().isDefined() else "interactive"
        notebook = str(ctx.notebookPath().get()) if ctx.notebookPath().isDefined() else "n/a"
        return {"job_id": job_id, "run_id": run_id, "task_key": task_key, "notebook": notebook}
    except Exception:
        return {"job_id": "manual", "run_id": "local", "task_key": "interactive", "notebook": "n/a"}


def get_logger(name: str, level: str = None, env: str = None) -> logging.Logger:
    """
    Returns a configured logger that auto-injects Databricks Job metadata and environment.
    
    Parameters
    ----------
    name : str
        Logger name (module or pipeline name).
    level : str
        Logging level, defaults to LOG_LEVEL env variable or INFO.
    env : str
        Environment name (dev/test/prod), defaults to "dev".
    """
    level = level or os.getenv("LOG_LEVEL", "INFO")
    env = env or os.getenv("ENV", "dev")

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(getattr(logging, level.upper(), logging.INFO))
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s env=%(env)s job_id=%(job_id)s run_id=%(run_id)s task_key=%(task_key)s notebook=%(notebook)s %(name)s - %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = False

    # Combine environment + auto job context
    context = {"env": env, **_get_job_context()}
    if not any(isinstance(f, DefaultContextFilter) for f in logger.filters):
        logger.addFilter(DefaultContextFilter(context))

    return logger


def create_tables(spark,schemas,database,clientId):
    for table, schema in schemas.items():
        table=table.split(".")[0]
        cols = [f"{f.name} {f.dataType.simpleString().upper()}" for f in schema.fields]
        spark.sql(f"""create table if not exists {database}.{clientId}_{table} (
            {", ".join(cols)}
        ) using delta """)
        print(f"{table} table  created successfully under {database} schema")


def run_scd_type2(spark, df_source, target_table_path, config_path):
    """
    Apply SCD Type 2 merge logic (update, insert, delete, soft delete).
    Config (YAML) defines keys, hash columns, audit fields, values, and options.
    """

    # Load config
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    keys = config["keys"]
    hash_cols = config["hash_columns"]

    audit_cols = config["audit_columns"]
    audit_vals = config["audit_values"]
    options = config.get("options", {})

    # Extract audit column names
    col_create_user = audit_cols["CreateUser"]
    col_create_date = audit_cols["CreateDate"]
    col_create_process = audit_cols["CreateProcess"]
    col_update_user = audit_cols["UpdateUser"]
    col_update_date = audit_cols["UpdateDate"]
    col_update_process = audit_cols["UpdateProcess"]
    col_start_date  = audit_cols["StartDate"]
    col_end_date    = audit_cols["EndDate"]
    col_active      = audit_cols["ActiveStatus"]

    # Extract audit values
    create_user_val = audit_vals["CreateUser"]
    update_user_val = audit_vals["UpdateUser"]
    active_val = audit_vals["ActiveStatus_active"]
    inactive_val = audit_vals["ActiveStatus_inactive"]

    # Load target delta table
    targetDelta = DeltaTable.forName(spark, target_table_path)
    df_target = targetDelta.toDF()

    # Compute hash_diff in source and target
    df_source_hashed = df_source.withColumn(
        "hash_diff",
        md5(concat_ws("||", *[df_source[c].cast("string") for c in hash_cols]))
    )

    df_target_hashed = df_target.withColumn(
        "hash_diff",
        md5(concat_ws("||", *[col(c).cast("string") for c in hash_cols]))
    )

    # Dynamic join condition on keys
    join_cond = [col(f"s.{k}") == col(f"t.{k}") for k in keys]
    full_join_cond = join_cond[0]
    for cond in join_cond[1:]:
        full_join_cond = full_join_cond & cond
    full_join_cond = full_join_cond & (col(f"t.{col_active}") == lit(active_val))

    # Join source & target
    joined_df = (
        df_source_hashed.alias("s")
        .join(df_target_hashed.alias("t"), full_join_cond, "leftouter")
        .select(col("s.*"), col("t.hash_diff").alias("t_hash_diff"))
    )

    # Define action splits
    update_df = joined_df.filter(col("s.hash_diff") != col("t_hash_diff")).withColumn("merge_key", lit("update"))
    insert_df = joined_df.filter(col("t_hash_diff").isNull()).withColumn("merge_key", lit("insert"))
    delete_df = joined_df.filter(col("s.hash_diff") != col("t_hash_diff")).withColumn("merge_key", lit("delete"))

    scd_df = delete_df.union(insert_df).union(update_df)

    # --- Soft delete handling ---
    if options.get("soft_delete", False):
        # Find active target records missing in source
        join_cond_soft = [col(f"s.{k}").isNull() & col(f"t.{k}").isNotNull() for k in keys]
        full_soft_cond = join_cond_soft[0]
        for cond in join_cond_soft[1:]:
            full_soft_cond = full_soft_cond & cond

        soft_delete_df = (
            df_target_hashed.alias("t")
            .join(df_source_hashed.alias("s"), full_join_cond, "leftouter")
            .filter(col("s.id").isNull() & (col(f"t.{col_active}") == lit(active_val)))
            .select([col(f"t.{k}") for k in keys])
            .withColumn("merge_key", lit("soft_delete"))
        )

        scd_df = scd_df.union(soft_delete_df)

    # Merge condition (dynamic on keys + active check)
    #merge_condition = " AND ".join([f"t.{k} = s.{k}" for k in keys]) + f" AND t.{col_active} = '{active_val}'"
    merge_condition = (
    (col(f"t.{keys[0]}") == col(f"s.{keys[0]}"))
    )
    for k in keys[1:]:
        merge_condition = merge_condition & (col(f"t.{k}") == col(f"s.{k}"))

    merge_condition = (
        merge_condition
        & (col(f"t.{col_active}") == lit(active_val))
        & (col("s.merge_key") == lit("delete"))
    )

    # Build insert/update value maps dynamically
    all_business_cols = hash_cols + keys
    update_values = {c: f"s.{c}" for c in all_business_cols} | {
        col_update_date: current_date(),
        col_update_user: lit(update_user_val),
        col_start_date: current_timestamp(),
        col_end_date: lit(None),
        col_active: lit(active_val)
    }
    insert_values = {c: f"s.{c}" for c in all_business_cols} | {
        col_create_date: current_timestamp(),
        col_create_user: lit(create_user_val),
        col_update_date: lit(None).cast("date"),
        col_update_user: lit(None),
        col_start_date: current_timestamp(),
        col_end_date: lit(None),
        col_active: lit(active_val)
    }

    # Perform merge
    (
        targetDelta.alias("t")
        .merge(scd_df.alias("s"), merge_condition)
        .whenMatchedUpdate(
            condition=(col("s.merge_key").isin("delete", "soft_delete")),
            set={
                col_active: lit(inactive_val),
                col_end_date: current_timestamp()
            }
        )
        .whenNotMatchedInsert(
            condition=(col("s.merge_key") == lit("update")),
            values=update_values
        )
        .whenNotMatchedInsert(
            condition=(col("s.merge_key") == lit("insert")),
            values=insert_values
        )
        .execute()
    )

    print("✅ SCD Type 2 Merge completed successfully (with soft delete).")

