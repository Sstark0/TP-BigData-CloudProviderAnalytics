"""
Esquemas explícitos para leer la zona Landing.

"""
from pyspark.sql.types import (DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType)
EVENTS_RAW_SCHEMA = StructType([
    StructField("event_id",           StringType(),  True),
    StructField("timestamp",          StringType(),  True),   
    StructField("org_id",             StringType(),  True),
    StructField("resource_id",        StringType(),  True),
    StructField("service",            StringType(),  True),
    StructField("region",             StringType(),  True),
    StructField("metric",             StringType(),  True),
    StructField("value",              StringType(),  True),   
    StructField("unit",               StringType(),  True),
    StructField("cost_usd_increment", StringType(),  True),   
    StructField("schema_version",     IntegerType(), True),
    StructField("carbon_kg",          StringType(),  True),   
    StructField("genai_tokens",       StringType(),  True),   
    StructField("_corrupt_record",    StringType(),  True),
])


def all_string_schema(columns):
    campos = []
    for columna in columns:
        campos.append(StructField(columna, StringType(), True))
    return StructType(campos)


# Columnas de cada CSV en el mismo orden que en el archivo
CSV_COLUMNS = {
    "customers_orgs": ["org_id", "org_name", "industry", "hq_region", "plan_tier",
                       "is_enterprise", "signup_date", "sales_rep", "lifecycle_stage",
                       "marketing_source", "nps_score"],
    "users": ["user_id", "org_id", "email", "role", "active", "created_at", "last_login"],
    "resources": ["resource_id", "org_id", "service", "region", "created_at", "state",
                  "tags_json"],
    "support_tickets": ["ticket_id", "org_id", "category", "severity", "created_at",
                        "resolved_at", "csat", "sla_breached"],
    "marketing_touches": ["touch_id", "org_id", "campaign", "channel", "timestamp",
                          "clicked", "converted"],
    "nps_surveys": ["org_id", "survey_date", "nps_score", "comment"],
    "billing_monthly": ["invoice_id", "org_id", "month", "subtotal", "credits", "taxes",
                        "currency", "exchange_rate_to_usd"],
}


NATURAL_KEYS = {
    "customers_orgs": ["org_id"],
    "users": ["user_id"],
    "resources": ["resource_id"],
    "support_tickets": ["ticket_id"],
    "marketing_touches": ["touch_id"],
    "nps_surveys": ["org_id", "survey_date"],
    "billing_monthly": ["invoice_id"],
    "usage_events": ["event_id"],
}

# Tipos de destino en Bronze.
# Lo que no se pueda convertir -> Quarantine
# Columnas que no figuran quedan como texto

BRONZE_TYPES = {
    "customers_orgs": {"is_enterprise": "boolean", "signup_date": "date", "nps_score": "double"},
    "users": {"active": "boolean", "created_at": "date", "last_login": "date"},
    "resources": {"created_at": "date"},
    "support_tickets": {"created_at": "date", "resolved_at": "date", "csat": "double", "sla_breached": "boolean"},
    "marketing_touches": {"timestamp": "date", "clicked": "boolean", "converted": "boolean"},
    "nps_surveys": {"survey_date": "date", "nps_score": "double"},
    "billing_monthly": {"month": "date", "subtotal": "decimal(12,2)", "credits": "decimal(12,2)",
                        "taxes": "decimal(12,2)", "exchange_rate_to_usd": "decimal(12,6)"},
}

# E2-03, D-12)
# Lo que escribe S1 y leen S2 (streaming) y B2 (reconciliación).
# Se declara explícito para que el stream de Silver pueda arrancar aunque Bronze todavía esté vacío.

BRONZE_EVENTS_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_ts", TimestampType()),
    StructField("event_date", DateType()),
    StructField("org_id", StringType()),
    StructField("resource_id", StringType()),
    StructField("service", StringType()),
    StructField("region", StringType()),
    StructField("metric", StringType()),
    StructField("value", DoubleType()),
    StructField("unit", StringType()),
    StructField("cost_usd_increment", DoubleType()),
    StructField("schema_version", IntegerType()),
    StructField("carbon_kg", DoubleType()),
    StructField("genai_tokens", LongType()),
    StructField("source_file", StringType()),
    StructField("arrival_ts", TimestampType()),
    StructField("ingest_ts", TimestampType()),
    StructField("run_id", StringType()),
    StructField("ingest_date", DateType()),      # partición
    StructField("batch_id", IntegerType()),      # partición
])
