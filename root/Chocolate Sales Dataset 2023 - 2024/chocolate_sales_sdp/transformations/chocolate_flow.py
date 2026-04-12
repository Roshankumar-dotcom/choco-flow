from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window


@dp.table(name = "calendar_bronze")
def calendar_bronze():
    df = spark.read.option("header" , "True").option("inferSchema" ,"True").csv("dbfs:/Volumes/chocolate_sales_kaggle/default/chocolate_sales_dataset_kaggle/source_kaggle/calendar.csv")
    df = df.withColumn("ingestion_time",current_timestamp())
    return df


@dp.table(name = "customer_bronze")
def customer_bronze():
    df = spark.read.option("header" , "True").option("inferSchema" ,"True").csv("dbfs:/Volumes/chocolate_sales_kaggle/default/chocolate_sales_dataset_kaggle/source_kaggle/customers.csv")
    df = df.withColumn("ingestion_time",current_timestamp())
    return df


@dp.table(name="product_bronze")
def product_bronze():
    df = spark.read.option("header" , "True").option("inferSchema" ,"True").csv("dbfs:/Volumes/chocolate_sales_kaggle/default/chocolate_sales_dataset_kaggle/source_kaggle/products.csv")
    df = df.withColumn("ingestion_time",current_timestamp())
    return df


@dp.table(name="sales_bronze")
def sales_bronze():
    df = spark.read.option("header" , "True").option("inferSchema" ,"True").csv("dbfs:/Volumes/chocolate_sales_kaggle/default/chocolate_sales_dataset_kaggle/source_kaggle/sales.csv")
    df = df.withColumn("ingestion_time",current_timestamp())
    return df


@dp.table(name="store_bronze")
def store_bronze():
    df = spark.read.option("header" , "True").option("inferSchema" ,"True").csv("dbfs:/Volumes/chocolate_sales_kaggle/default/chocolate_sales_dataset_kaggle/source_kaggle/stores.csv")
    df = df.withColumn("ingestion_time",current_timestamp())
    return df

@dp.table(name="calendar_silver",comment="Cleaned and enriched calendar dimension table")
def calendar_silver():
    df = spark.read.table("calendar_bronze")
    df = df.withColumn("date",to_date(col("date"),"MM/dd/yyyy"))

    df = df.filter(col("date").isNotNull())
    
    df = df.dropDuplicates(['date'])

    df = df.withColumn("year", year("date")) \
           .withColumn("month", month("date")) \
           .withColumn("day", dayofmonth("date")) \
           .withColumn("day_of_week", dayofweek("date")) \
           .withColumn("week", weekofyear("date")) \
           .withColumn("day_of_year", dayofyear("date")) \
           .withColumn("quarter", quarter("date")) \
           .withColumn("is_weekend", when(col("day_of_week").isin(1,7), True).otherwise(False)) \
           .withColumn("is_weekday", when(col("day_of_week").between(2,6), True).otherwise(False))
    return df

@dp.table(name="customer_silver",comment="Cleaned and enriched customer dimention table")
def customer_silver():
    df = spark.read.table("customer_bronze")
    df = df.dropDuplicates(["customer_id"])
    df = df.filter(col("customer_id").isNotNull() & (col("customer_id") != ""))
    df = df.filter((col("age") >= 0) & (col("age") <= 100))
    df = df.withColumn("gender",trim(lower("gender")))
    df = df.withColumn("gender",
                       when(col("gender").isin("male","m") , "Male")
                       .when(col("gender").isin("female","f"),"Female")
                       .otherwise("Other")
                       )
    df = df.withColumn("is_loyalty_member",
                       when(col("loyalty_member")== 1 , True).otherwise(False)).drop("loyalty_member")
    df = df.filter(col("join_date").isNotNull())
    df = df.withColumn("membership_days", datediff(current_date(),col("join_date")))
    df = df.withColumn("age_group", 
                       when(col("age") < 18 ,"Under 18")
                       .when(col("age").between(18,25),"18-25")
                       .when(col("age").between(26,40),"26-40")
                       .when(col("age").between(41,60),"41-60")
                       .otherwise("60+")
                       )
    return df


@dp.table(
    name="product_silver",
    comment="Cleaned and validated product dimension table"
)
@dp.expect("valid_product_id", "product_id IS NOT NULL")
@dp.expect("valid_cocoa_percent", "cocoa_percent BETWEEN 0 AND 100")
@dp.expect("valid_weight", "weight_g > 0")
def product_silver():

    df = spark.read.table("product_bronze")
    df = df.filter(col("product_id").isNotNull() & (col("product_id") != ""))

    window_spec = Window.partitionBy("product_id").orderBy(col("ingestion_time").desc())

    df = df.withColumn("row_num", row_number().over(window_spec)) \
           .filter(col("row_num") == 1) \
           .drop("row_num")

    df = df.withColumn("product_name", initcap(trim(col("product_name")))) \
           .withColumn("brand", upper(trim(col("brand")))) \
           .withColumn("category", initcap(trim(col("category"))))

    df = df.fillna({
        "brand": "UNKNOWN",
        "category": "Other"
    })

    df = df.filter((col("cocoa_percent") >= 0) & (col("cocoa_percent") <= 100))

    df = df.filter((col("weight_g") > 0) & (col("weight_g") <= 5000))

    df = df.withColumn(
        "cocoa_level",
        when(col("cocoa_percent") < 40, "Low")
        .when(col("cocoa_percent").between(40, 70), "Medium")
        .otherwise("High")
    )

    df = df.withColumn(
        "weight_category",
        when(col("weight_g") < 100, "Small")
        .when(col("weight_g").between(100, 300), "Medium")
        .otherwise("Large")
    )

    return df

@dp.table(
    name="store_silver",
    comment="Cleaned and standardized store dimension table"
)
@dp.expect("valid_store_id", "store_id IS NOT NULL")
def store_silver():

    df = spark.read.table("store_bronze")

    df = df.filter(col("store_id").isNotNull() & (col("store_id") != ""))

    window_spec = Window.partitionBy("store_id").orderBy(col("ingestion_time").desc())

    df = df.withColumn("row_num", row_number().over(window_spec)) \
           .filter(col("row_num") == 1) \
           .drop("row_num")

    df = df.withColumn("store_name", initcap(trim(col("store_name")))) \
           .withColumn("city", initcap(trim(col("city")))) \
           .withColumn("country", upper(trim(col("country")))) \
           .withColumn("store_type", initcap(trim(col("store_type"))))

    df = df.fillna({
        "city": "Unknown",
        "country": "UNKNOWN",
        "store_type": "Other"
    })

    df = df.withColumn(
        "store_type",
        when(col("store_type").isin("Online", "Retail", "Outlet"), col("store_type"))
        .otherwise("Other")
    )

    df = df.withColumn(
        "region",
        when(col("country") == "USA", "North America")
        .when(col("country") == "INDIA", "Asia")
        .otherwise("Other")
    )

    return df

@dp.table(
    name="sales_silver",
    comment="Cleaned and validated sales fact table"
)
@dp.expect("valid_order_id", "order_id IS NOT NULL")
@dp.expect("valid_quantity", "quantity > 0")
@dp.expect("valid_unit_price", "unit_price > 0")
def sales_silver():

    df = spark.read.table("sales_bronze")

    df = df.filter(col("order_id").isNotNull() & (col("order_id") != ""))

    window_spec = Window.partitionBy("order_id").orderBy(col("ingestion_time").desc())

    df = df.withColumn("row_num", row_number().over(window_spec)) \
           .filter(col("row_num") == 1) \
           .drop("row_num")

    df = df.filter(
        col("product_id").isNotNull() &
        col("store_id").isNotNull() &
        col("customer_id").isNotNull()
    )

    df = df.filter(
        (col("quantity") > 0) &
        (col("unit_price") > 0) &
        (col("discount").between(0, 1)) &
        (col("cost") >= 0)
    )

    df = df.withColumn(
        "calculated_revenue",
        col("quantity") * col("unit_price") * (1 - col("discount"))
    )

    df = df.withColumn(
        "calculated_profit",
        col("calculated_revenue") - col("cost")
    )

    df = df.drop("revenue", "profit") \
           .withColumnRenamed("calculated_revenue", "revenue") \
           .withColumnRenamed("calculated_profit", "profit")

    df = df.filter(col("order_date").isNotNull())
    df = df.filter(col("order_date") <= current_date())

    df = df.withColumn("year", year("order_date")) \
           .withColumn("month", month("order_date")) \
           .withColumn("quarter", quarter("order_date"))

    return df


@dp.table(name="sales_gold")
def sales_gold():

    sales = spark.read.table("sales_silver")
    product = spark.read.table("product_silver")
    customer = spark.read.table("customer_silver")
    store = spark.read.table("store_silver")
    calendar = spark.read.table("calendar_silver")

    df = sales.alias("s") \
        .join(product.alias("p"), "product_id", "left") \
        .join(customer.alias("c"), "customer_id", "left") \
        .join(store.alias("st"), "store_id", "left") \
        .join(calendar.alias("cal"), col("s.order_date") == col("cal.date"), "left")

    return df.select(
        # Sales fields
        "s.customer_id",
        "s.product_id",
        "s.store_id",
        "s.order_id",
        "s.order_date",
        "s.quantity",
        "s.unit_price",
        "s.discount",
        "s.revenue",
        "s.cost",
        "s.profit",

        # Product fields
        "p.product_name",
        "p.brand",
        "p.category",
        "p.cocoa_level",
        "p.weight_category",

        # Customer fields
        "c.age_group",
        "c.gender",
        "c.is_loyalty_member",

        # Store fields
        "st.city",
        "st.country",
        "st.store_type",
        "st.region",

        # Calendar fields
        col("cal.year").alias("order_year"),
        col("cal.month").alias("order_month"),
        col("cal.quarter").alias("order_quarter"),
        col("cal.week").alias("order_week")
    )

@dp.table(name="kpi_summary_gold")
def kpi_summary_gold():

    df = spark.read.table("sales_silver")

    return df.agg(
        sum("revenue").alias("total_revenue"),
        sum("profit").alias("total_profit"),
        sum("quantity").alias("total_quantity"),
        avg("discount").alias("avg_discount")
    )

@dp.table(name="revenue_by_month_gold")
def revenue_by_month_gold():

    df = spark.read.table("sales_silver")

    return df.groupBy("year", "month") \
             .agg(
                 sum("revenue").alias("monthly_revenue"),
                 sum("profit").alias("monthly_profit")
             )

@dp.table(name="product_performance_gold")
def product_performance_gold():

    df = spark.read.table("sales_gold")

    return df.groupBy("product_name", "brand", "category") \
             .agg(
                 sum("revenue").alias("total_revenue"),
                 sum("profit").alias("total_profit"),
                 sum("quantity").alias("total_quantity")
             )

@dp.table(name="customer_segmentation_gold")
def customer_segmentation_gold():

    df = spark.read.table("sales_gold")

    return df.groupBy("age_group", "is_loyalty_member") \
             .agg(
                 sum("revenue").alias("revenue"),
                 countDistinct("customer_id").alias("customer_count")
             )

@dp.table(name="store_performance_gold")
def store_performance_gold():

    df = spark.read.table("sales_gold")

    return df.groupBy("country", "city", "store_type") \
             .agg(
                 sum("revenue").alias("total_revenue"),
                 sum("profit").alias("total_profit")
             )



